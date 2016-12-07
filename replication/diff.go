package replication

import (
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

func parseTxtTime(filename string) (time.Time, error) {
	ds, err := state.ParseFile(filename)
	if err != nil {
		return time.Time{}, err
	}
	return ds.Time, nil
}
