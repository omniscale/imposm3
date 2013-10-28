package main

import (
	"fmt"
	golog "log"
	"os"
	"path"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"imposm3/cache"
	"imposm3/cache/query"
	"imposm3/config"
	"imposm3/database"
	_ "imposm3/database/postgis"
	"imposm3/diff"
	state "imposm3/diff/state"
	"imposm3/geom/limit"
	"imposm3/logging"
	"imposm3/mapping"
	"imposm3/parser/pbf"
	"imposm3/reader"
	"imposm3/stats"
	"imposm3/writer"
)

var log = logging.NewLogger("")

func printCmds() {
	fmt.Fprintf(os.Stderr, "Usage: %s COMMAND [args]\n\n", os.Args[0])
	fmt.Println("Available commands:")
	fmt.Println("\timport")
	fmt.Println("\tdiff")
	fmt.Println("\tquery-cache")
	fmt.Println("\tversion")
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
		config.ParseImport(os.Args[2:])
		mainimport()
	case "diff":
		config.ParseDiffImport(os.Args[2:])

		var geometryLimiter *limit.Limiter
		if config.BaseOptions.LimitTo != "" {
			var err error
			step := log.StartStep("Reading limitto geometries")
			geometryLimiter, err = limit.NewFromGeoJsonWithBuffered(
				config.BaseOptions.LimitTo,
				config.BaseOptions.LimitToCacheBuffer,
			)
			if err != nil {
				log.Fatal(err)
			}
			log.StopStep(step)
		}

		for _, oscFile := range config.DiffFlags.Args() {
			diff.Update(oscFile, geometryLimiter, false)
		}
	case "query-cache":
		query.Query(os.Args[2:])
	case "version":
		fmt.Println(imposmVersion)
		os.Exit(0)
	default:
		printCmds()
		log.Fatalf("invalid command: '%s'", os.Args[1])
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

	var geometryLimiter *limit.Limiter
	if config.ImportOptions.Write && config.BaseOptions.LimitTo != "" {
		var err error
		step := log.StartStep("Reading limitto geometries")
		geometryLimiter, err = limit.NewFromGeoJson(config.BaseOptions.LimitTo)
		if err != nil {
			log.Fatal(err)
		}
		log.StopStep(step)
	}

	tagmapping, err := mapping.NewMapping(config.BaseOptions.MappingFile)
	if err != nil {
		log.Fatal("mapping file: ", err)
	}

	var db database.DB

	if config.ImportOptions.Write || config.ImportOptions.DeployProduction || config.ImportOptions.RevertDeploy || config.ImportOptions.RemoveBackup || config.ImportOptions.Optimize {
		if config.BaseOptions.Connection == "" {
			log.Fatal("missing connection option")
		}
		connType := database.ConnectionType(config.BaseOptions.Connection)
		conf := database.Config{
			Type:             connType,
			ConnectionParams: config.BaseOptions.Connection,
			Srid:             config.BaseOptions.Srid,
		}
		db, err = database.Open(conf, tagmapping)
		if err != nil {
			log.Fatal(err)
		}
	}

	osmCache := cache.NewOSMCache(config.BaseOptions.CacheDir)

	if config.ImportOptions.Read != "" && osmCache.Exists() {
		if config.ImportOptions.Overwritecache {
			log.Printf("removing existing cache %s", config.BaseOptions.CacheDir)
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
				diffstate.WriteToFile(path.Join(config.BaseOptions.CacheDir, "last.state.txt"))
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
			diffCache = cache.NewDiffCache(config.BaseOptions.CacheDir)
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
		if diffCache != nil {
			diffCache.Coords.SetLinearImport(true)
			diffCache.Ways.SetLinearImport(true)
		}
		osmCache.Coords.SetReadOnly(true)

		relations := osmCache.Relations.Iter()
		relWriter := writer.NewRelationWriter(osmCache, diffCache, relations,
			db, progress, config.BaseOptions.Srid)
		relWriter.SetLimiter(geometryLimiter)
		relWriter.Start()

		// blocks till the Relations.Iter() finishes
		relWriter.Close()
		osmCache.Relations.Close()

		ways := osmCache.Ways.Iter()
		wayWriter := writer.NewWayWriter(osmCache, diffCache, ways, db,
			progress, config.BaseOptions.Srid)
		wayWriter.SetLimiter(geometryLimiter)
		wayWriter.Start()

		// blocks till the Ways.Iter() finishes
		wayWriter.Close()
		osmCache.Ways.Close()

		nodes := osmCache.Nodes.Iter()
		nodeWriter := writer.NewNodeWriter(osmCache, nodes, db,
			progress, config.BaseOptions.Srid)
		nodeWriter.SetLimiter(geometryLimiter)
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
