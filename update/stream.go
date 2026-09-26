package update

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/omniscale/go-osm/replication"
	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/config"
	"github.com/omniscale/imposm3/database"
	"github.com/omniscale/imposm3/expire"
	"github.com/omniscale/imposm3/geom/limit"
	"github.com/omniscale/imposm3/log"
	"github.com/omniscale/imposm3/mapping"
)

type streamBatch struct {
	ID    uint64   `json:"id"`
	Files []string `json:"files"`
}

func decodeStreamBatch(data []byte) (streamBatch, error) {
	var batch streamBatch
	if err := json.Unmarshal(data, &batch); err != nil {
		return batch, err
	}
	if batch.ID == 0 || len(batch.Files) == 0 || len(batch.Files) > 64 {
		return batch, fmt.Errorf("expected a positive id and 1..64 diff files")
	}
	seen := make(map[string]bool)
	for _, file := range batch.Files {
		if !strings.HasSuffix(file, ".osc.gz") || seen[file] {
			return batch, fmt.Errorf("invalid or duplicate diff filename")
		}
		seen[file] = true
	}
	return batch, nil
}

// processStream keeps the pipe protocol separate from importer resources.
// apply must finish all database/cache/checkpoint work before returning success.
func processStream(input io.Reader, output io.Writer, apply func([]string) error) error {
	writer := bufio.NewWriter(output)
	fmt.Fprintln(writer, "READY imposm-stream-v1")
	if err := writer.Flush(); err != nil {
		return err
	}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for scanner.Scan() {
		batch, err := decodeStreamBatch(scanner.Bytes())
		if err != nil {
			return fmt.Errorf("parsing diff-stream request: %w", err)
		}
		if err := apply(batch.Files); err != nil {
			return fmt.Errorf("importing diff-stream batch %d: %w", batch.ID, err)
		}
		fmt.Fprintf(writer, "OK %d\n", batch.ID)
		if err := writer.Flush(); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// Stream imports coordinator-supplied batches while retaining the database
// connection, mapping, geographic limiter and LevelDB handles/block caches.
// It does not download diffs or advance without another request. A coordinator
// should wait for OK before reading derived data, then close stdin for shutdown.
// Database commits and the separate caches are not one atomic transaction.
func Stream(opts config.Base, input io.Reader, output io.Writer) error {
	if opts.Quiet {
		log.SetMinLevel(log.LInfo)
	}
	var limiter *limit.Limiter
	var err error
	if opts.LimitTo != "" {
		limiter, err = limit.NewFromGeoJSON(opts.LimitTo, opts.LimitToCacheBuffer, opts.Srid)
		if err != nil {
			return err
		}
	}
	osmCache := cache.NewOSMCache(opts.CacheDir)
	if err := osmCache.Open(); err != nil {
		return err
	}
	defer osmCache.Close()
	diffCache := cache.NewDiffCache(opts.CacheDir)
	if err := diffCache.Open(); err != nil {
		return err
	}
	defer diffCache.Close()
	tagmapping, err := mapping.FromFile(opts.MappingFile)
	if err != nil {
		return err
	}
	db, err := database.Open(database.Config{
		ConnectionParams: opts.Connection,
		Srid:             opts.Srid,
		ImportSchema:     opts.Schemas.Production,
		ProductionSchema: opts.Schemas.Production,
		BackupSchema:     opts.Schemas.Backup,
	}, &tagmapping.Conf)
	if err != nil {
		return err
	}
	defer db.Close()
	fullDB, ok := db.(database.FullDB)
	if !ok {
		return fmt.Errorf("database does not support diff imports")
	}

	var tilelist *expire.TileList
	var expireor expire.Expireor
	if opts.ExpireTilesDir != "" {
		tilelist = expire.NewTileList(opts.ExpireTilesZoom, opts.ExpireTilesDir)
		expireor = tilelist
	}
	if err := cache.SyncBarrier(osmCache, diffCache); err != nil {
		return err
	}
	lastStateFile := filepath.Join(opts.DiffDir, LastStateFilename)
	return processStream(input, output, func(files []string) error {
		nextSeq, err := sequenceFromFiles(files, lastStateFile, opts.ForceDiffImport)
		if err != nil {
			return err
		}
		inTransaction := false
		defer func() {
			if inTransaction {
				fullDB.Abort()
			}
		}()
		var last replication.Sequence
		commit := func() error {
			if err := fullDB.End(); err != nil {
				return err
			}
			inTransaction = false
			if err := osmCache.Coords.Flush(); err != nil {
				return err
			}
			diffCache.Flush()
			return markImported(last, lastStateFile)
		}
		for seq := range nextSeq {
			if !inTransaction {
				if err := fullDB.Begin(); err != nil {
					return err
				}
				inTransaction = true
			}
			if err := importDiffFile(seq.Filename, fullDB, tagmapping, opts.Srid, limiter, expireor, osmCache, diffCache); err != nil {
				return err
			}
			last = seq
			if !opts.CommitLatest {
				if err := commit(); err != nil {
					return err
				}
			}
		}
		if inTransaction {
			if err := commit(); err != nil {
				return err
			}
		}
		if last.Filename == "" { // all files were already imported
			return syncStreamState(lastStateFile)
		}
		if err := cache.SyncBarrier(osmCache, diffCache); err != nil {
			return err
		}
		if err := syncStreamState(lastStateFile); err != nil {
			return err
		}
		if tilelist != nil {
			return tilelist.Flush()
		}
		return nil
	})
}

// Make the final state file and directory entry durable before acknowledging.
func syncStreamState(path string) error {
	for _, name := range []string{path, filepath.Dir(path)} {
		f, err := os.Open(name)
		if err != nil {
			return err
		}
		err = f.Sync()
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}
