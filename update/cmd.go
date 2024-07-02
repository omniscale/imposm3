package update

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/omniscale/go-osm/replication"
	"github.com/omniscale/go-osm/replication/diff"
	"github.com/omniscale/go-osm/state"
	diffstate "github.com/omniscale/go-osm/state"
	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/config"
	"github.com/omniscale/imposm3/database"
	"github.com/omniscale/imposm3/expire"
	"github.com/omniscale/imposm3/geom/limit"
	"github.com/omniscale/imposm3/log"
	"github.com/omniscale/imposm3/mapping"
	"github.com/pkg/errors"
)

func Diff(baseOpts config.Base, files []string) {
	if baseOpts.Quiet {
		log.SetMinLevel(log.LInfo)
	}

	nextSeq, err := sequenceFromFiles(
		files,
		filepath.Join(baseOpts.DiffDir, LastStateFilename),
		baseOpts.ForceDiffImport,
	)
	if err != nil {
		log.Fatalf("[error] Checking diff files: %v", err)
	}

	if err := diffImportLoop(baseOpts, nextSeq, commitEach, false); err != nil {
		log.Fatalf("[error] Importing diffs: %v", err)
	}
}

func Run(baseOpts config.Base) {
	if baseOpts.Quiet {
		log.SetMinLevel(log.LInfo)
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
	defer downloader.Stop()

	if err := diffImportLoop(baseOpts, nextSeq, commitLatest, true); err != nil {
		log.Fatalf("[error] Importing diffs: %v", err)
	}
}

type commitMode int

const (
	commitEach commitMode = iota
	commitLatest
)

func diffImportLoop(baseOpts config.Base, nextSeq <-chan replication.Sequence, commit commitMode, run bool) error {
	var geometryLimiter *limit.Limiter
	if baseOpts.LimitTo != "" {
		var err error
		logReadLimitTo := log.Step("Reading limitto geometries")
		geometryLimiter, err = limit.NewFromGeoJSON(
			baseOpts.LimitTo,
			baseOpts.LimitToCacheBuffer,
			baseOpts.Srid,
		)
		if err != nil {
			return errors.Wrapf(err, "reading limit to geometry")
		}
		logReadLimitTo()
	}

	osmCache := cache.NewOSMCache(baseOpts.CacheDir)
	if err := osmCache.Open(); err != nil {
		return errors.Wrapf(err, "opening OSM cache")
	}
	defer osmCache.Close()

	diffCache := cache.NewDiffCache(baseOpts.CacheDir)
	if err := diffCache.Open(); err != nil {
		return errors.Wrapf(err, "opening diff cache")
	}
	defer diffCache.Close()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	var tilelist *expire.TileList
	var tileExpireor expire.Expireor
	if baseOpts.ExpireTilesDir != "" {
		tilelist = expire.NewTileList(baseOpts.ExpireTilesZoom, baseOpts.ExpireTilesDir)
		tileExpireor = tilelist
	}

	tagmapping, err := mapping.FromFile(baseOpts.MappingFile)
	if err != nil {
		log.Fatalf("[fatal] reading tagmapping: %v", err)
	}
	var lastStateFile = filepath.Join(baseOpts.DiffDir, LastStateFilename)

	dbConf := database.Config{
		ConnectionParams: baseOpts.Connection,
		Srid:             baseOpts.Srid,
		// we apply diff imports on the Production schema
		ImportSchema:     baseOpts.Schemas.Production,
		ProductionSchema: baseOpts.Schemas.Production,
		BackupSchema:     baseOpts.Schemas.Backup,
	}
	db, err := database.Open(dbConf, &tagmapping.Conf)
	if err != nil {
		log.Fatalf("[fatal] unable to open database: %v", err)
	}
	defer db.Close()

	if err := db.Begin(); err != nil {
		log.Fatalf("[fatal] unable to start transaction: %v", err)
	}

	var lastDiffFile string
	flush := func() error {
		if tilelist != nil {
			err := tilelist.Flush()
			if err != nil {
				log.Println("[error] Writing tile expire list", err)
			}
		}
		if err := db.End(); err != nil {
			return errors.Wrapf(err, "unable to commit transaction")
		}
		if err := db.Begin(); err != nil {
			return errors.Wrapf(err, "unable to start transaction")
		}
		if err := osmCache.Coords.Flush(); err != nil {
			return errors.Wrapf(err, "flushing coords cache")
		}
		diffCache.Flush()

		if lastDiffFile != "" {
			if err := markImported(lastDiffFile, lastStateFile); err != nil {
				log.Println("[error] Unable to write last state:", err)
			}
		}
		return nil
	}

	shutdown := func() error {
		osmCache.Close()
		diffCache.Close()
		if err := db.Abort(); err != nil {
			return err
		}
		if err := db.Close(); err != nil {
			return err
		}
		return nil
	}

	exp := newExpBackoff(2*time.Second, 5*time.Minute)

	for {
		select {
		case <-sigc:
			log.Println("[info] Exiting. (SIGTERM/SIGINT/SIGHUP)")
			return shutdown()
		case seq := <-nextSeq:
			if seq.Error != nil {
				log.Printf("[error] Get seq #%d: %s", seq.Sequence, seq.Error)
				continue
			}

			if seq.Filename == "" { // seq is zero-value if channel was closed (when all files are imported)
				if err := flush(); err != nil {
					if err := shutdown(); err != nil {
						log.Printf("[error] error during shutdown for following error: %v", err)
					}
					return err
				}
				return shutdown()
			}

			lastDiffFile = seq.Filename // for last.state.txt update in Flush

			logName := seq.Filename
			if seq.Sequence != 0 {
				logName = "#" + strconv.FormatInt(int64(seq.Sequence), 10)
			}

			for {
				if seq.Sequence != 0 {
					log.Printf("[info] Importing %s including changes till %s (%s behind)",
						logName, seq.Time, time.Since(seq.Time).Truncate(time.Second))
				} else {
					log.Printf("[info] Importing %s", logName)
				}

				logFinishedImport := log.Step(fmt.Sprintf("Importing %s", logName))
				err := Update(baseOpts, seq.Filename, db.(database.FullDB),
					tagmapping, geometryLimiter, tileExpireor,
					osmCache, diffCache,
				)
				if err == nil {
					exp.Reset()
					if seq.Latest || commit == commitEach {
						if err := flush(); err != nil {
							return err
						}
					}
				}
				logFinishedImport()

				if err != nil {
					if commit == commitEach {
						// we can retry if we commited the previous import
						log.Printf("[error] Importing %s: %v", logName, err)
						log.Println("[info] Retrying in", exp.Duration())
						select {
						case <-sigc:
							log.Println("[info] Exiting. (SIGTERM/SIGINT/SIGHUP)")
							return shutdown()
						case <-exp.Wait():
						default:
						}
						continue
					} else {
						// terminate on error, systemd/etc. should restart imposm
						if err := shutdown(); err != nil {
							log.Printf("[error] error during shutdown for following error: %v", err)
						}
						return err
					}
				}
				break
			}
			if os.Getenv("IMPOSM3_SINGLE_DIFF") != "" {
				if err := flush(); err != nil {
					if err := shutdown(); err != nil {
						log.Printf("[error] error during shutdown for following error: %v", err)
					}
					return err
				}
				return shutdown()
			}
		}
	}
}

func sequenceFromFiles(files []string, lastStateFile string, force bool) (<-chan replication.Sequence, error) {
	lastState, err := diffstate.ParseFile(lastStateFile)
	if err != nil && !force {
		log.Printf("[info] Unable to read last state, will not check if already imported: %v", err)
	}

	c := make(chan replication.Sequence, len(files))

	for i, oscFile := range files {
		var state *diffstate.DiffState
		if strings.HasSuffix(oscFile, ".osc.gz") {
			stateFile := oscFile[:len(oscFile)-len(".osc.gz")] + ".state.txt"
			var err error
			state, err = diffstate.ParseFile(stateFile)
			if err != nil && !os.IsNotExist(err) {
				return nil, errors.Wrapf(err, "reading state %s", stateFile)
			}
		}

		if state != nil && lastState != nil {
			if state.Sequence <= lastState.Sequence && !force {
				log.Printf("[info] Skipping %d (%v), already imported", state.Sequence, state.Time)
				continue
			}
		}

		latest := i == len(files)-1

		if state != nil {
			c <- replication.Sequence{
				Sequence: state.Sequence,
				Filename: oscFile,
				Time:     state.Time,
				Latest:   latest,
			}
		} else {
			c <- replication.Sequence{
				Filename: oscFile,
				Latest:   latest,
			}
		}
	}
	close(c)

	return c, nil
}

func markImported(oscFile string, lastStateFile string) error {
	if !strings.HasSuffix(oscFile, ".osc.gz") {
		return errors.New("diff file requires .osc.gz suffix")
	}
	stateFile := oscFile[:len(oscFile)-len(".osc.gz")] + ".state.txt"
	state, err := diffstate.ParseFile(stateFile)
	if err != nil {
		return errors.Wrapf(err, "reading state %s", stateFile)
	}

	lastState, err := diffstate.ParseFile(lastStateFile)
	if err == nil {
		state.URL = lastState.URL
	}

	err = diffstate.WriteFile(lastStateFile, state)
	if err != nil {
		return errors.Wrapf(err, "unable to write last state")
	}
	return nil
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

func (eb *expBackoff) Wait() <-chan time.Time {
	timer := time.After(eb.current)
	eb.current = eb.current * 2
	if eb.current > eb.max {
		eb.current = eb.max
	}
	return timer
}

func (eb *expBackoff) Reset() {
	eb.current = eb.min
}
