package diff

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/config"
	"github.com/omniscale/imposm3/diff/download"
	"github.com/omniscale/imposm3/expire"
	"github.com/omniscale/imposm3/geom/limit"
	"github.com/omniscale/imposm3/logging"
)

var logger = logging.NewLogger("")

func Run() {
	if config.BaseOptions.Quiet {
		logging.SetQuiet(true)
	}

	var geometryLimiter *limit.Limiter
	if config.BaseOptions.LimitTo != "" {
		var err error
		step := logger.StartStep("Reading limitto geometries")
		geometryLimiter, err = limit.NewFromGeoJSON(
			config.BaseOptions.LimitTo,
			config.BaseOptions.LimitToCacheBuffer,
			config.BaseOptions.Srid,
		)
		if err != nil {
			logger.Fatal(err)
		}
		logger.StopStep(step)
	}
	downloader, err := download.NewDiffDownload(config.BaseOptions.DiffDir,
		config.BaseOptions.ReplicationUrl, config.BaseOptions.ReplicationInterval)
	if err != nil {
		logger.Fatal("unable to start diff downloader", err)
	}

	osmCache := cache.NewOSMCache(config.BaseOptions.CacheDir)
	err = osmCache.Open()
	if err != nil {
		logger.Fatal("osm cache: ", err)
	}
	defer osmCache.Close()

	diffCache := cache.NewDiffCache(config.BaseOptions.CacheDir)
	err = diffCache.Open()
	if err != nil {
		logger.Fatal("diff cache: ", err)
	}
	defer diffCache.Close()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGTERM, syscall.SIGHUP)

	shutdown := func() {
		logger.Print("Exiting. (SIGTERM/SIGHUB)")
		logging.Shutdown()
		osmCache.Close()
		diffCache.Close()
		os.Exit(0)
	}

	var tiles *expire.TileList
	var tileExpireor expire.Expireor
	if config.BaseOptions.ExpireTilesDir != "" {
		tiles = expire.NewTileList(config.BaseOptions.ExpireTilesZoom, config.BaseOptions.ExpireTilesDir)
		tileExpireor = tiles
	}

	exp := newExpBackoff(2*time.Second, 5*time.Minute)

	for {
		select {
		case <-sigc:
			shutdown()
		case nextDiff := <-downloader.NextDiff:
			fname := nextDiff.FileName
			state := nextDiff.State
			for {
				p := logger.StartStep(fmt.Sprintf("importing #%d till %s", state.Sequence, state.Time))

				err := Update(fname, geometryLimiter, tileExpireor, osmCache, diffCache, false)

				osmCache.Coords.Flush()
				diffCache.Flush()

				if err == nil && tiles != nil {
					err := tiles.Flush()
					if err != nil {
						logger.Print("error writing tile expire list", err)
					}
				}

				logger.StopStep(p)

				select {
				case <-sigc:
					shutdown()
				default:
				}

				if err != nil {
					logger.Error(err)
					logger.Print("retrying in ", exp.Duration())
					exp.Wait()
				} else {
					exp.Reset()
					break
				}
			}
			if os.Getenv("IMPOSM3_SINGLE_DIFF") != "" {
				return
			}
		}
	}
}

type expBackoff struct {
	current time.Duration
	min     time.Duration
	max     time.Duration
}

func newExpBackoff(min, max time.Duration) *expBackoff {
	return &expBackoff{min, min, max}
}

func (eb *expBackoff) Duration() time.Duration {
	return eb.current
}

func (eb *expBackoff) Wait() {
	time.Sleep(eb.current)
	eb.current = eb.current * 2
	if eb.current > eb.max {
		eb.current = eb.max
	}
}

func (eb *expBackoff) Reset() {
	eb.current = eb.min
}
