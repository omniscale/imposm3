package update

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/config"
	"github.com/omniscale/imposm3/expire"
	"github.com/omniscale/imposm3/geom/limit"
	"github.com/omniscale/imposm3/logging"
	"github.com/omniscale/imposm3/replication"
	"github.com/omniscale/imposm3/update/state"
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

	s, err := state.ParseLastState(config.BaseOptions.DiffDir)
	if err != nil {
		log.Fatal("unable to read last.state.txt", err)
	}
	replicationUrl := config.BaseOptions.ReplicationUrl
	if replicationUrl == "" {
		replicationUrl = s.Url
	}
	if replicationUrl == "" {
		log.Fatal("no replicationUrl in last.state.txt " +
			"or replication_url in -config file")
	}

	downloader := replication.NewDiffDownloader(
		config.BaseOptions.DiffDir,
		replicationUrl,
		s.Sequence,
		config.BaseOptions.ReplicationInterval,
	)
	nextSeq := downloader.Sequences()

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
	signal.Notify(sigc, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	var tilelist *expire.TileList
	var lastTlFlush = time.Now()
	var tileExpireor expire.Expireor
	if config.BaseOptions.ExpireTilesDir != "" {
		tilelist = expire.NewTileList(config.BaseOptions.ExpireTilesZoom, config.BaseOptions.ExpireTilesDir)
		tileExpireor = tilelist
	}

	shutdown := func() {
		logger.Print("Exiting. (SIGTERM/SIGINT/SIGHUB)")
		logging.Shutdown()
		osmCache.Close()
		diffCache.Close()
		if tilelist != nil {
			err := tilelist.Flush()
			if err != nil {
				logger.Print("error writing tile expire list", err)
			}
		}
		os.Exit(0)
	}

	exp := newExpBackoff(2*time.Second, 5*time.Minute)

	for {
		select {
		case <-sigc:
			shutdown()
		case seq := <-nextSeq:
			fname := seq.Filename
			seqId := seq.Sequence
			seqTime := seq.Time
			for {
				p := logger.StartStep(fmt.Sprintf("importing #%d till %s", seqId, seqTime))

				err := Update(fname, geometryLimiter, tileExpireor, osmCache, diffCache, false)

				osmCache.Coords.Flush()
				diffCache.Flush()

				if err == nil && tilelist != nil && time.Since(lastTlFlush) > time.Second*30 {
					// call at most once every 30 seconds to reduce files during the
					// catch-up phase after the initial import
					lastTlFlush = time.Now()
					err := tilelist.Flush()
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
