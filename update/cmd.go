package update

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/omniscale/go-osm/replication/diff"
	"github.com/omniscale/go-osm/state"
	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/config"
	"github.com/omniscale/imposm3/expire"
	"github.com/omniscale/imposm3/geom/limit"
	"github.com/omniscale/imposm3/log"
)

func Run(baseOpts config.Base) {
	if baseOpts.Quiet {
		log.SetMinLevel(log.LInfo)
	}

	var geometryLimiter *limit.Limiter
	if baseOpts.LimitTo != "" {
		var err error
		step := log.Step("Reading limitto geometries")
		geometryLimiter, err = limit.NewFromGeoJSON(
			baseOpts.LimitTo,
			baseOpts.LimitToCacheBuffer,
			baseOpts.Srid,
		)
		if err != nil {
			log.Fatal("[error] Reading limit to geometry", err)
		}
		step()
	}

	s, err := state.ParseFile(filepath.Join(baseOpts.DiffDir, LastStateFilename))
	if err != nil {
		log.Fatal("[fatal] Unable to read last.state.txt:", err)
	}
	replicationURL := baseOpts.ReplicationURL
	if replicationURL == "" {
		replicationURL = s.URL
	}
	if replicationURL == "" {
		log.Fatal("[fatal] No replicationURL in last.state.txt " +
			"or replication_url in -config")
	}
	log.Printf("[info] Starting replication from %s with %s interval", replicationURL, baseOpts.ReplicationInterval)

	downloader := diff.NewDownloader(
		baseOpts.DiffDir,
		replicationURL,
		s.Sequence+1,
		baseOpts.ReplicationInterval,
	)
	nextSeq := downloader.Sequences()

	osmCache := cache.NewOSMCache(baseOpts.CacheDir)
	err = osmCache.Open()
	if err != nil {
		log.Fatal("[fatal] Opening OSM cache:", err)
	}
	defer osmCache.Close()

	diffCache := cache.NewDiffCache(baseOpts.CacheDir)
	err = diffCache.Open()
	if err != nil {
		log.Fatal("[fatal] Opening diff cache:", err)
	}
	defer diffCache.Close()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	var tilelist *expire.TileList
	var lastTlFlush = time.Now()
	var tileExpireor expire.Expireor
	if baseOpts.ExpireTilesDir != "" {
		tilelist = expire.NewTileList(baseOpts.ExpireTilesZoom, baseOpts.ExpireTilesDir)
		tileExpireor = tilelist
	}

	shutdown := func() {
		log.Println("[info] Exiting. (SIGTERM/SIGINT/SIGHUP)")
		downloader.Stop()
		osmCache.Close()
		diffCache.Close()
		if tilelist != nil {
			err := tilelist.Flush()
			if err != nil {
				log.Println("[error] Writing tile expire list", err)
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
			if seq.Error != nil {
				log.Printf("[error] Downloading #%d: %s", seq.Sequence, seq.Error)
				continue
			}
			fname := seq.Filename
			seqID := seq.Sequence
			seqTime := seq.Time
			for {
				log.Printf("[info] Importing #%d including changes till %s (%s behind)", seqID, seqTime, time.Since(seqTime).Truncate(time.Second))
				finishedImport := log.Step(fmt.Sprintf("Importing #%d", seqID))

				err := Update(baseOpts, fname, geometryLimiter, tileExpireor, osmCache, diffCache, false)

				osmCache.Coords.Flush()
				diffCache.Flush()

				if err == nil && tilelist != nil && time.Since(lastTlFlush) > time.Second*30 {
					// call at most once every 30 seconds to reduce files during the
					// catch-up phase after the initial import
					lastTlFlush = time.Now()
					err := tilelist.Flush()
					if err != nil {
						log.Println("[error] Writing tile expire list", err)
					}
				}

				finishedImport()

				select {
				case <-sigc:
					shutdown()
				default:
				}

				if err != nil {
					log.Printf("[error] Importing #%d: %s", seqID, err)
					log.Println("[info] Retrying in", exp.Duration())
					// TODO handle <-sigc during wait
					exp.Wait()
				} else {
					exp.Reset()
					break
				}
			}
			if os.Getenv("IMPOSM3_SINGLE_DIFF") != "" {
				shutdown()
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
