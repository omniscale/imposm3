package main

import (
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

func die(args ...interface{}) {
	log.Fatal(args...)
}

func dief(msg string, args ...interface{}) {
	log.Fatalf(msg, args...)
}

func reportErrors(errs []error) {
	log.Warn("errors in config/options:")
	for _, err := range errs {
		log.Warnf("\t%s", err)
	}
	logging.Shutdown()
	os.Exit(1)
}

func main() {

	golog.SetFlags(golog.LstdFlags | golog.Lshortfile)
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	if len(os.Args) <= 1 {
		os.Exit(1)
	}

	switch os.Args[1] {
	case "import":
		errs := config.ParseImport(os.Args[2:])
		if len(errs) > 0 {
			reportErrors(errs)
			break
		}
		mainimport()
	case "diff":
		errs := config.ParseDiffImport(os.Args[2:])
		if len(errs) > 0 {
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
		die("-revertdeploy and -removebackup not compatible with -read/-write")
	}

	if config.ImportOptions.RevertDeploy && (config.ImportOptions.RemoveBackup || config.ImportOptions.DeployProduction) {
		die("-revertdeploy not compatible with -deployproduction/-removebackup")
	}

	var geometryClipper *clipper.Clipper
	if config.ImportOptions.Write && config.ImportOptions.Base.LimitTo != "" {
		var err error
		step := log.StartStep("Reading limitto geometries")
		geometryClipper, err = clipper.NewFromOgrSource(config.ImportOptions.Base.LimitTo)
		if err != nil {
			die(err)
		}
		log.StopStep(step)
	}

	osmCache := cache.NewOSMCache(config.ImportOptions.Base.CacheDir)

	if config.ImportOptions.Read != "" && osmCache.Exists() {
		if config.ImportOptions.Overwritecache {
			log.Printf("removing existing cache %s", config.ImportOptions.Base.CacheDir)
			err := osmCache.Remove()
			if err != nil {
				die("unable to remove cache:", err)
			}
		} else if !config.ImportOptions.Appendcache {
			die("cache already exists use -appendcache or -overwritecache")
		}
	}

	progress := stats.StatsReporter()

	tagmapping, err := mapping.NewMapping(config.ImportOptions.Base.MappingFile)
	if err != nil {
		die("mapping file: ", err)
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
			die(err)
		}
	}

	step := log.StartStep("Imposm")

	if config.ImportOptions.Read != "" {
		step := log.StartStep("Reading OSM data")
		err = osmCache.Open()
		if err != nil {
			die(err)
		}
		progress.Start()

		pbfFile, err := pbf.Open(config.ImportOptions.Read)
		if err != nil {
			log.Fatal(err)
		}

		osmCache.Coords.SetLinearImport(true)
		reader.ReadPbf(osmCache, progress, tagmapping, pbfFile)
		osmCache.Coords.SetLinearImport(false)
		progress.Stop()
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
		progress.Start()
		err = db.Init()
		if err != nil {
			die(err)
		}

		bulkDb, ok := db.(database.BulkBeginner)
		if ok {
			err = bulkDb.BeginBulk()
		} else {
			err = db.Begin()
		}
		if err != nil {
			die(err)
		}

		var diffCache *cache.DiffCache
		if config.ImportOptions.Diff {
			diffCache = cache.NewDiffCache(config.ImportOptions.Base.CacheDir)
			if err = diffCache.Remove(); err != nil {
				die(err)
			}
			if err = diffCache.Open(); err != nil {
				die(err)
			}
		}

		err = osmCache.Open()
		if err != nil {
			die(err)
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
			die(err)
		}

		progress.Stop()

		if config.ImportOptions.Diff {
			diffCache.Close()
		}

		log.StopStep(stepWrite)

		if db, ok := db.(database.Generalizer); ok {
			if err := db.Generalize(); err != nil {
				die(err)
			}
		} else {
			die("database not generalizeable")
		}

		if db, ok := db.(database.Finisher); ok {
			if err := db.Finish(); err != nil {
				die(err)
			}
		} else {
			die("database not finishable")
		}
		log.StopStep(stepImport)
	}

	if config.ImportOptions.Optimize {
		if db, ok := db.(database.Optimizer); ok {
			if err := db.Optimize(); err != nil {
				die(err)
			}
		} else {
			die("database not optimizable")
		}
	}

	if config.ImportOptions.DeployProduction {
		if db, ok := db.(database.Deployer); ok {
			if err := db.Deploy(); err != nil {
				die(err)
			}
		} else {
			die("database not deployable")
		}
	}

	if config.ImportOptions.RevertDeploy {
		if db, ok := db.(database.Deployer); ok {
			if err := db.RevertDeploy(); err != nil {
				die(err)
			}
		} else {
			die("database not deployable")
		}
	}

	if config.ImportOptions.RemoveBackup {
		if db, ok := db.(database.Deployer); ok {
			if err := db.RemoveBackup(); err != nil {
				die(err)
			}
		} else {
			die("database not deployable")
		}
	}

	log.StopStep(step)

}
