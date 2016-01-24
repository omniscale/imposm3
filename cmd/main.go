package cmd

import (
	"fmt"
	golog "log"
	"os"
	"runtime"

	"github.com/jmhodges/levigo"

	"github.com/omniscale/imposm3/cache/query"
	"github.com/omniscale/imposm3/config"
	"github.com/omniscale/imposm3/diff"
	"github.com/omniscale/imposm3/geom/geos"
	"github.com/omniscale/imposm3/import_"
	"github.com/omniscale/imposm3/logging"
	"github.com/omniscale/imposm3/stats"
)

var log = logging.NewLogger("")

func PrintCmds() {
	fmt.Fprintf(os.Stderr, "Usage: %s COMMAND [args]\n\n", os.Args[0])
	fmt.Println("Available commands:")
	fmt.Println("\timport")
	fmt.Println("\tdiff")
	fmt.Println("\tquery-cache")
	fmt.Println("\tversion")
	fmt.Println("See more: http://imposm.org/\n")
}

func Main(usage func()) {
	golog.SetFlags(golog.LstdFlags | golog.Lshortfile)
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	if len(os.Args) <= 1 {
		usage()
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
		diff.Diff()

	case "query-cache":
		query.Query(os.Args[2:])
	case "version":
		fmt.Printf("%s %s(%s-%s-%s)", Version, runtime.Version(), runtime.GOARCH, runtime.GOOS, runtime.Compiler)
		fmt.Printf(" geos=(%s)", geos.Version())
		fmt.Printf(" leveldb=%d.%d", levigo.GetLevelDBMajorVersion(), levigo.GetLevelDBMinorVersion())
		fmt.Printf(" numcpu=%d\n", runtime.NumCPU())
		os.Exit(0)
	default:
		usage()
		log.Fatalf("invalid command: '%s'", os.Args[1])
	}
	logging.Shutdown()
	os.Exit(0)

}
