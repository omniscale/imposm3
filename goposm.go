package main

import (
	"fmt"
	"goposm/cache"
	"goposm/config"
	"goposm/database"
	_ "goposm/database/postgis"
	"goposm/diff"
	state "goposm/diff/state"
	"goposm/geom/clipper"
	"goposm/logging"
	"goposm/mapping"
	"goposm/parser/pbf"
	"goposm/reader"
	"goposm/stats"
	"goposm/writer"
	golog "log"
	"os"
	"path"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"
)

var log = logging.NewLogger("")

func reportErrors(errs []error) {
	fmt.Println("errors in config/options:")
	for _, err := range errs {
		fmt.Printf("\t%s\n", err)
	}
	logging.Shutdown()
	os.Exit(1)
}

func printCmds() {
	fmt.Println("available commands:")
	fmt.Println("\timport")
	fmt.Println("\tdiff")
}

func main() {

	golog.SetFlags(golog.LstdFlags | golog.Lshortfile)
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	if len(os.Args) <= 1 {
		printCmds()
		logging.Shutdown()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "import":
		errs := config.ParseImport(os.Args[2:])
		if len(errs) > 0 {
			config.ImportFlags.PrintDefaults()
			reportErrors(errs)
			break
		}
		mainimport()
	case "diff":
		errs := config.ParseDiffImport(os.Args[2:])
		if len(errs) > 0 {
			config.DiffImportFlags.PrintDefaults()
			reportErrors(errs)
			break
		}
		for _, oscFile := range config.DiffImportFlags.Args() {
			diff.Update(oscFile, false)
		}
	default:
		log.Fatal("invalid command")
	}
	logging.Shutdown()
	os.Exit(0)

}

func mainimport() {
	if config.ImportOptions.Cpuprofile != "" {
		f, err := os.Create(config.ImportOptions.Cpuprofile)
		if err != nil {
			golog.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if config.ImportOptions.Httpprofile != "" {
		stats.StartHttpPProf(config.ImportOptions.Httpprofile)
	}

	if config.ImportOptions.Memprofile != "" {
		parts := strings.Split(config.ImportOptions.Memprofile, string(os.PathListSeparator))
		var interval time.Duration

		if len(parts) < 2 {
			interval, _ = time.ParseDuration("1m")
		} else {
			var err error
			interval, err = time.ParseDuration(parts[1])
			if err != nil {
				golog.Fatal(err)
			}
		}

		go stats.MemProfiler(parts[0], interval)
	}

	if config.ImportOptions.Quiet {
		logging.SetQuiet(true)
	}

	if (config.ImportOptions.Write || config.ImportOptions.Read != "") && (config.ImportOptions.RevertDeploy || config.ImportOptions.RemoveBackup) {
		log.Fatal("-revertdeploy and -removebackup not compatible with -read/-write")
	}

	if config.ImportOptions.RevertDeploy && (config.ImportOptions.RemoveBackup || config.ImportOptions.DeployProduction) {
		log.Fatal("-revertdeploy not compatible with -deployproduction/-removebackup")
	}

	var geometryClipper *clipper.Clipper
	if config.ImportOptions.Write && config.ImportOptions.Base.LimitTo != "" {
		var err error
		step := log.StartStep("Reading limitto geometries")
		geometryClipper, err = clipper.NewFromOgrSource(config.ImportOptions.Base.LimitTo)
		if err != nil {
			log.Fatal(err)
		}
		log.StopStep(step)
	}

	tagmapping, err := mapping.NewMapping(config.ImportOptions.Base.MappingFile)
	if err != nil {
		log.Fatal("mapping file: ", err)
	}

	var db database.DB

	if config.ImportOptions.Write || config.ImportOptions.DeployProduction || config.ImportOptions.RevertDeploy || config.ImportOptions.RemoveBackup || config.ImportOptions.Optimize {
		connType := database.ConnectionType(config.ImportOptions.Base.Connection)
		conf := database.Config{
			Type:             connType,
			ConnectionParams: config.ImportOptions.Base.Connection,
			Srid:             config.ImportOptions.Base.Srid,
		}
		db, err = database.Open(conf, tagmapping)
		if err != nil {
			log.Fatal(err)
		}
	}

	osmCache := cache.NewOSMCache(config.ImportOptions.Base.CacheDir)

	if config.ImportOptions.Read != "" && osmCache.Exists() {
		if config.ImportOptions.Overwritecache {
			log.Printf("removing existing cache %s", config.ImportOptions.Base.CacheDir)
			err := osmCache.Remove()
			if err != nil {
				log.Fatal("unable to remove cache:", err)
			}
		} else if !config.ImportOptions.Appendcache {
			log.Fatal("cache already exists use -appendcache or -overwritecache")
		}
	}

	step := log.StartStep("Imposm")

	var elementCounts *stats.ElementCounts

	if config.ImportOptions.Read != "" {
		step := log.StartStep("Reading OSM data")
		err = osmCache.Open()
		if err != nil {
			log.Fatal(err)
		}
		progress := stats.NewStatsReporter()

		pbfFile, err := pbf.Open(config.ImportOptions.Read)
		if err != nil {
			log.Fatal(err)
		}

		osmCache.Coords.SetLinearImport(true)
		reader.ReadPbf(osmCache, progress, tagmapping, pbfFile)
		osmCache.Coords.SetLinearImport(false)
		elementCounts = progress.Stop()
		osmCache.Close()
		log.StopStep(step)
		if config.ImportOptions.Diff {
			diffstate := state.FromPbf(pbfFile)
			if diffstate != nil {
				diffstate.WriteToFile(path.Join(config.ImportOptions.Base.CacheDir, "last.state.txt"))
			}
		}
	}

	if config.ImportOptions.Write {
		stepImport := log.StartStep("Importing OSM data")
		stepWrite := log.StartStep("Writing OSM data")
		progress := stats.NewStatsReporterWithEstimate(elementCounts)

		err = db.Init()
		if err != nil {
			log.Fatal(err)
		}

		bulkDb, ok := db.(database.BulkBeginner)
		if ok {
			err = bulkDb.BeginBulk()
		} else {
			err = db.Begin()
		}
		if err != nil {
			log.Fatal(err)
		}

		var diffCache *cache.DiffCache
		if config.ImportOptions.Diff {
			diffCache = cache.NewDiffCache(config.ImportOptions.Base.CacheDir)
			if err = diffCache.Remove(); err != nil {
				log.Fatal(err)
			}
			if err = diffCache.Open(); err != nil {
				log.Fatal(err)
			}
		}

		err = osmCache.Open()
		if err != nil {
			log.Fatal(err)
		}
		osmCache.Coords.SetReadOnly(true)
		pointsTagMatcher := tagmapping.PointMatcher()
		lineStringsTagMatcher := tagmapping.LineStringMatcher()
		polygonsTagMatcher := tagmapping.PolygonMatcher()

		relations := osmCache.Relations.Iter()
		relWriter := writer.NewRelationWriter(osmCache, diffCache, relations,
			db, polygonsTagMatcher, progress, config.ImportOptions.Base.Srid)
		relWriter.SetClipper(geometryClipper)
		relWriter.Start()

		// blocks till the Relations.Iter() finishes
		relWriter.Close()
		osmCache.Relations.Close()

		ways := osmCache.Ways.Iter()
		wayWriter := writer.NewWayWriter(osmCache, diffCache, ways, db,
			lineStringsTagMatcher, polygonsTagMatcher, progress, config.ImportOptions.Base.Srid)
		wayWriter.SetClipper(geometryClipper)
		wayWriter.Start()

		// blocks till the Ways.Iter() finishes
		wayWriter.Close()
		osmCache.Ways.Close()

		nodes := osmCache.Nodes.Iter()
		nodeWriter := writer.NewNodeWriter(osmCache, nodes, db,
			pointsTagMatcher, progress, config.ImportOptions.Base.Srid)
		nodeWriter.SetClipper(geometryClipper)
		nodeWriter.Start()

		// blocks till the Nodes.Iter() finishes
		nodeWriter.Close()
		osmCache.Close()

		err = db.End()
		if err != nil {
			log.Fatal(err)
		}

		progress.Stop()

		if config.ImportOptions.Diff {
			diffCache.Close()
		}

		log.StopStep(stepWrite)

		if db, ok := db.(database.Generalizer); ok {
			if err := db.Generalize(); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("database not generalizeable")
		}

		if db, ok := db.(database.Finisher); ok {
			if err := db.Finish(); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("database not finishable")
		}
		log.StopStep(stepImport)
	}

	if config.ImportOptions.Optimize {
		if db, ok := db.(database.Optimizer); ok {
			if err := db.Optimize(); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("database not optimizable")
		}
	}

	if config.ImportOptions.DeployProduction {
		if db, ok := db.(database.Deployer); ok {
			if err := db.Deploy(); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("database not deployable")
		}
	}

	if config.ImportOptions.RevertDeploy {
		if db, ok := db.(database.Deployer); ok {
			if err := db.RevertDeploy(); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("database not deployable")
		}
	}

	if config.ImportOptions.RemoveBackup {
		if db, ok := db.(database.Deployer); ok {
			if err := db.RemoveBackup(); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("database not deployable")
		}
	}

	log.StopStep(step)

}
