package main

import (
	"fmt"
	golog "log"
	"os"
	"runtime"

	"imposm3/cache/query"
	"imposm3/config"
	"imposm3/diff"
	"imposm3/geom/limit"
	"imposm3/import_"
	"imposm3/logging"
	"imposm3/stats"
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
		if config.BaseOptions.Httpprofile != "" {
			stats.StartHttpPProf(config.BaseOptions.Httpprofile)
		}
		import_.Import()
	case "diff":
		config.ParseDiffImport(os.Args[2:])
		if config.BaseOptions.Httpprofile != "" {
			stats.StartHttpPProf(config.BaseOptions.Httpprofile)
		}
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
			diff.Update(oscFile, geometryLimiter, nil, false)
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
