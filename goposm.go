package main

import (
	"flag"
	"goposm/cache"
	"goposm/config"
	"goposm/database"
	_ "goposm/database/postgis"
	diffstate "goposm/diff"
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

var (
	cpuprofile  = flag.String("cpuprofile", "", "filename of cpu profile output")
	httpprofile = flag.String("httpprofile", "", "bind address for profile server")
	memprofile  = flag.String("memprofile", "", "dir name of mem profile output and interval (fname:interval)")

	overwritecache = flag.Bool("overwritecache", false, "overwritecache")
	appendcache    = flag.Bool("appendcache", false, "append cache")

	read     = flag.String("read", "", "read")
	write    = flag.Bool("write", false, "write")
	optimize = flag.Bool("optimize", false, "optimize")
	diff     = flag.Bool("diff", false, "enable diff support")

	deployProduction = flag.Bool("deployproduction", false, "deploy production")
	revertDeploy     = flag.Bool("revertdeploy", false, "revert deploy to production")
	removeBackup     = flag.Bool("removebackup", false, "remove backups from deploy")

	quiet = flag.Bool("quiet", false, "quiet log output")
)

func die(args ...interface{}) {
	log.Fatal(args...)
}

func dief(msg string, args ...interface{}) {
	log.Fatalf(msg, args...)
}

func main() {
	golog.SetFlags(golog.LstdFlags | golog.Lshortfile)
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}
	flag.Parse()
	conf, errs := config.Parse()
	if len(errs) > 0 {
		log.Warn("errors in config/options:")
		for _, err := range errs {
			log.Warnf("\t%s", err)
		}
		logging.Shutdown()
		os.Exit(1)
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			golog.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if *httpprofile != "" {
		stats.StartHttpPProf(*httpprofile)
	}

	if *memprofile != "" {
		parts := strings.Split(*memprofile, string(os.PathListSeparator))
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

	if *quiet {
		logging.SetQuiet(true)
	}

	if (*write || *read != "") && (*revertDeploy || *removeBackup) {
		die("-revertdeploy and -removebackup not compatible with -read/-write")
	}

	if *revertDeploy && (*removeBackup || *deployProduction) {
		die("-revertdeploy not compatible with -deployproduction/-removebackup")
	}

	var geometryClipper *clipper.Clipper
	if *write && conf.LimitTo != "" {
		var err error
		step := log.StartStep("Reading limitto geometries")
		geometryClipper, err = clipper.NewFromOgrSource(conf.LimitTo)
		if err != nil {
			die(err)
		}
		log.StopStep(step)
	}

	osmCache := cache.NewOSMCache(conf.CacheDir)

	if *read != "" && osmCache.Exists() {
		if *overwritecache {
			log.Printf("removing existing cache %s", conf.CacheDir)
			err := osmCache.Remove()
			if err != nil {
				die("unable to remove cache:", err)
			}
		} else if !*appendcache {
			die("cache already exists use -appendcache or -overwritecache")
		}
	}

	progress := stats.StatsReporter()

	tagmapping, err := mapping.NewMapping(conf.MappingFile)
	if err != nil {
		die("mapping file: ", err)
	}

	var db database.DB

	if *write || *deployProduction || *revertDeploy || *removeBackup || *optimize {
		connType := database.ConnectionType(conf.Connection)
		conf := database.Config{
			Type:             connType,
			ConnectionParams: conf.Connection,
			Srid:             conf.Srid,
		}
		db, err = database.Open(conf, tagmapping)
		if err != nil {
			die(err)
		}
	}

	step := log.StartStep("Imposm")

	if *read != "" {
		step := log.StartStep("Reading OSM data")
		err = osmCache.Open()
		if err != nil {
			die(err)
		}
		progress.Start()

		pbfFile, err := pbf.Open(*read)
		if err != nil {
			log.Fatal(err)
		}

		osmCache.Coords.SetLinearImport(true)
		reader.ReadPbf(osmCache, progress, tagmapping, pbfFile)
		osmCache.Coords.SetLinearImport(false)
		progress.Stop()
		osmCache.Close()
		log.StopStep(step)
		if *diff {
			state := diffstate.StateFromPbf(pbfFile)
			if state != nil {
				state.WriteToFile(path.Join(conf.CacheDir, "last.state.txt"))
			}
		}
	}

	if *write {
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
		if *diff {
			diffCache = cache.NewDiffCache(conf.CacheDir)
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
			db, polygonsTagMatcher, progress, conf.Srid)
		relWriter.SetClipper(geometryClipper)
		relWriter.Start()

		// blocks till the Relations.Iter() finishes
		relWriter.Close()
		osmCache.Relations.Close()

		ways := osmCache.Ways.Iter()
		wayWriter := writer.NewWayWriter(osmCache, diffCache, ways, db,
			lineStringsTagMatcher, polygonsTagMatcher, progress, conf.Srid)
		wayWriter.SetClipper(geometryClipper)
		wayWriter.Start()

		// blocks till the Ways.Iter() finishes
		wayWriter.Close()
		osmCache.Ways.Close()

		nodes := osmCache.Nodes.Iter()
		nodeWriter := writer.NewNodeWriter(osmCache, nodes, db,
			pointsTagMatcher, progress, conf.Srid)
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

		if *diff {
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

	if *optimize {
		if db, ok := db.(database.Optimizer); ok {
			if err := db.Optimize(); err != nil {
				die(err)
			}
		} else {
			die("database not optimizable")
		}
	}

	if *deployProduction {
		if db, ok := db.(database.Deployer); ok {
			if err := db.Deploy(); err != nil {
				die(err)
			}
		} else {
			die("database not deployable")
		}
	}

	if *revertDeploy {
		if db, ok := db.(database.Deployer); ok {
			if err := db.RevertDeploy(); err != nil {
				die(err)
			}
		} else {
			die("database not deployable")
		}
	}

	if *removeBackup {
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
