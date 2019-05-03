package diff

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/omniscale/go-osm/replication"
	"github.com/omniscale/go-osm/replication/internal/source"
	"github.com/omniscale/go-osm/state"
)

// NewDownloader starts a background downloader for OSM diff files (.osc.gz).
// Diffs are fetched from url and stored in diffDir. seq is the first
// sequence that should be downloaded. Diffs are downloaded as fast as
// possible with a single connection until the first diff is missing.
// After that, it uses the interval to estimate when a new diff should
// appear. The returned replication.Source provides metadata for each
// downloaded diff.
func NewDownloader(diffDir, url string, seq int, interval time.Duration) replication.Source {
	dl := source.NewDownloader(diffDir, url, seq, interval)
	dl.FileExt = ".osc.gz"
	dl.StateExt = ".state.txt"
	dl.StateTime = parseTxtTime
	go dl.Start()
	return dl
}

// CurrentSequence returns the ID of the latest diff available at the
// given replication URL (e.g.
// https://planet.openstreetmap.org/replication/minute/)
func CurrentSequence(replURL string) (int, error) {
	resp, err := http.Get(replURL + "state.txt")
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != 200 {
		return 0, errors.New(fmt.Sprintf("invalid repsonse: %v", resp))
	}
	defer resp.Body.Close()
	s, err := state.Parse(resp.Body)
	if err != nil {
		return 0, err
	}
	return s.Sequence, nil
}

func parseTxtTime(filename string) (time.Time, error) {
	ds, err := state.ParseFile(filename)
	if err != nil {
		return time.Time{}, err
	}
	return ds.Time, nil
}

// NewReader starts a goroutine to search for OSM diff files (.osc.gz).
// This can be used if another tool is already downloading diff files (e.g.
// Imposm). Diffs are searched in diffDir. seq is the first sequence that
// should be returned. Diffs are returned as fast as possible if they are
// already available in diffDir. After that, it uses file change notifications
// provided by your OS to detect new diff files. The returned
// replication.Source provides metadata for each downloaded diff.
func NewReader(diffDir string, seq int) replication.Source {
	r := source.NewReader(diffDir, seq)
	r.FileExt = ".osc.gz"
	r.StateExt = ".state.txt"
	r.StateTime = parseTxtTime
	go r.Start()
	return r
}
