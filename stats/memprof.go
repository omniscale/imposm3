package stats

import (
	"fmt"
	"os"
	"path"
	"runtime/pprof"
	"time"

	"github.com/omniscale/imposm3/log"
)

func MemProfiler(dir string, interval time.Duration) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		panic(err)
	}

	ticker := time.NewTicker(interval)
	i := 0
	for _ = range ticker.C {
		filename := path.Join(
			dir,
			fmt.Sprintf("memprof-%03d.pprof", i),
		)
		f, err := os.Create(filename)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
		i++
	}
}
