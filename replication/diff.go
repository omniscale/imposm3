package replication

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/omniscale/imposm3/update/state"
)

func NewDiffDownloader(dest, url string, seq int, interval time.Duration) *downloader {
	dl := newDownloader(dest, url, seq, interval)
	dl.fileExt = ".osc.gz"
	dl.stateExt = ".state.txt"
	dl.stateTime = parseTxtTime
	go dl.fetchNextLoop()
	return dl
}

func CurrentDiff(url string) (int, error) {
	resp, err := http.Get(url + "state.txt")
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

func NewDiffReader(dest string, seq int) *reader {
	r := newReader(dest, seq)
	r.fileExt = ".osc.gz"
	r.stateExt = ".state.txt"
	r.stateTime = parseTxtTime
	go r.fetchNextLoop()
	return r
}
