package main

import (
	"flag"
	"goposm/cache"
	"goposm/database"
	_ "goposm/database/postgis"
	"goposm/geom/clipper"
	"goposm/logging"
	"goposm/mapping"
	"goposm/reader"
	"goposm/stats"
	"goposm/writer"
	golog "log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"
)

var dbImportBatchSize int64

var log = logging.NewLogger("")

func init() {
	dbImportBatchSize, _ = strconv.ParseInt(
		os.Getenv("GOPOSM_DBIMPORT_BATCHSIZE"), 10, 32)

	if dbImportBatchSize == 0 {
		dbImportBatchSize = 4096
	}
}

var (
	cpuprofile       = flag.String("cpuprofile", "", "filename of cpu profile output")
	httpprofile      = flag.String("httpprofile", "", "bind address for profile server")
	memprofile       = flag.String("memprofile", "", "dir name of mem profile output and interval (fname:interval)")
	cachedir         = flag.String("cachedir", "/tmp/goposm", "cache directory")
	overwritecache   = flag.Bool("overwritecache", false, "overwritecache")
	appendcache      = flag.Bool("appendcache", false, "append cache")
	read             = flag.String("read", "", "read")
	write            = flag.Bool("write", false, "write")
	connection       = flag.String("connection", "", "connection parameters")
	diff             = flag.Bool("diff", false, "enable diff support")
	mappingFile      = flag.String("mapping", "", "mapping file")
	deployProduction = flag.Bool("deployproduction", false, "deploy production")
	revertDeploy     = flag.Bool("revertdeploy", false, "revert deploy to production")
	removeBackup     = flag.Bool("removebackup", false, "remove backups from deploy")
	limitTo          = flag.String("limitto", "", "limit to geometries")
	quiet            = flag.Bool("quiet", false, "quiet log output")
)

func die(args ...interface{}) {
	log.Fatal(args...)
	logging.Shutdown()
	os.Exit(1)
}

func dief(msg string, args ...interface{}) {
	log.Fatalf(msg, args...)
	logging.Shutdown()
	os.Exit(1)
}

func main() {
	golog.SetFlags(golog.LstdFlags | golog.Lshortfile)
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

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
	if *write && *limitTo != "" {
		var err error
		step := log.StartStep("Reading limitto geometries")
		geometryClipper, err = clipper.NewFromOgrSource(*limitTo)
		if err != nil {
			die(err)
		}
		log.StopStep(step)
	}

	osmCache := cache.NewOSMCache(*cachedir)

	if *read != "" && osmCache.Exists() {
		if *overwritecache {
			log.Printf("removing existing cache %s", *cachedir)
			err := osmCache.Remove()
			if err != nil {
				die("unable to remove cache:", err)
			}
		} else if !*appendcache {
			die("cache already exists use -appendcache or -overwritecache")
		}
	}

	err := osmCache.Open()
	if err != nil {
		die(err)
	}
	defer osmCache.Close()

	progress := stats.StatsReporter()

	tagmapping, err := mapping.NewMapping(*mappingFile)
	if err != nil {
		die(err)
	}

	var db database.DB

	if *write || *deployProduction || *revertDeploy || *removeBackup {
		connType := database.ConnectionType(*connection)
		conf := database.Config{
			Type:             connType,
			ConnectionParams: *connection,
			Srid:             3857,
		}
		db, err = database.Open(conf, tagmapping)
		if err != nil {
			die(err)
		}
	}

	step := log.StartStep("Imposm")

	if *read != "" {
		step := log.StartStep("Reading OSM data")
		progress.Start()
		osmCache.Coords.SetLinearImport(true)
		reader.ReadPbf(osmCache, progress, tagmapping, *read)
		osmCache.Coords.SetLinearImport(false)
		progress.Stop()
		osmCache.Coords.Flush()
		log.StopStep(step)
	}

	if *write {
		stepImport := log.StartStep("Importing OSM data")
		stepWrite := log.StartStep("Writing OSM data")
		progress.Start()
		err = db.Init()
		if err != nil {
			die(err)
		}

		err = db.Begin()
		if err != nil {
			die(err)
		}
		var diffCache *cache.DiffCache

		if *diff {
			diffCache = cache.NewDiffCache(*cachedir)
			if err = diffCache.Remove(); err != nil {
				die(err)
			}
			if err = diffCache.Open(); err != nil {
				die(err)
			}
		}

		pointsTagMatcher := tagmapping.PointMatcher()
		lineStringsTagMatcher := tagmapping.LineStringMatcher()
		polygonsTagMatcher := tagmapping.PolygonMatcher()

		relations := osmCache.Relations.Iter()
		relWriter := writer.NewRelationWriter(osmCache, diffCache, relations,
			db, polygonsTagMatcher, progress)
		relWriter.SetClipper(geometryClipper)
		relWriter.Start()

		// blocks till the Relations.Iter() finishes
		relWriter.Close()

		ways := osmCache.Ways.Iter()
		wayWriter := writer.NewWayWriter(osmCache, diffCache, ways, db,
			lineStringsTagMatcher, polygonsTagMatcher, progress)
		wayWriter.SetClipper(geometryClipper)
		wayWriter.Start()

		// blocks till the Ways.Iter() finishes
		wayWriter.Close()

		nodes := osmCache.Nodes.Iter()
		nodeWriter := writer.NewNodeWriter(osmCache, nodes, db,
			pointsTagMatcher, progress)
		nodeWriter.SetClipper(geometryClipper)
		nodeWriter.Start()

		// blocks till the Nodes.Iter() finishes
		nodeWriter.Close()

		err = db.End()
		if err != nil {
			die(err)
		}

		// insertBuffer.Close()
		// dbWriter.Close()
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
