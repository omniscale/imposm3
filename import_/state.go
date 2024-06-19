package import_

import (
	"fmt"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/omniscale/go-osm/parser/pbf"
	"github.com/omniscale/go-osm/state"
	"github.com/pkg/errors"
)

func estimateFromPBF(filename string, before time.Duration, replicationURL string, replicationInterval time.Duration) (*state.DiffState, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, errors.Wrap(err, "opening PBF file")
	}
	defer f.Close()

	pbfparser := pbf.New(f, pbf.Config{})
	header, err := pbfparser.Header()

	var timestamp time.Time
	if err == nil && header.Time.Unix() > 0 {
		timestamp = header.Time
	} else {
		fstat, err := os.Stat(filename)
		if err != nil {
			return nil, errors.Wrapf(err, "reading mod time from %q", filename)
		}
		timestamp = fstat.ModTime()
	}
	return estimateFromTimestamp(timestamp, before, replicationURL, replicationInterval)
}

func estimateFromTimestamp(timestamp time.Time, before time.Duration, replicationURL string, replicationInterval time.Duration) (*state.DiffState, error) {
	if replicationURL == "" {
		switch replicationInterval {
		case time.Hour:
			replicationURL = "https://planet.openstreetmap.org/replication/hour/"
		case time.Hour * 24:
			replicationURL = "https://planet.openstreetmap.org/replication/day/"
		default:
			replicationURL = "https://planet.openstreetmap.org/replication/minute/"
		}
	}
	seq, err := estimateSequence(replicationURL, replicationInterval, timestamp)
	if err != nil {
		return nil, errors.Wrap(err, "fetching current sequence for estimated import sequence")
	}

	// start earlier
	seq -= int(math.Ceil(before.Minutes() / replicationInterval.Minutes()))
	return &state.DiffState{Time: timestamp, URL: replicationURL, Sequence: seq}, nil
}

func currentState(url string) (*state.DiffState, error) {
	resp, err := http.Get(url + "state.txt")
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, errors.New(fmt.Sprintf("invalid response: %v", resp))
	}
	defer resp.Body.Close()
	return state.Parse(resp.Body)
}

func estimateSequence(url string, interval time.Duration, timestamp time.Time) (int, error) {
	state, err := currentState(url)
	if err != nil {
		// discard first error and try a second time before failing
		time.Sleep(time.Second * 2)
		state, err = currentState(url)
		if err != nil {
			return 0, errors.Wrap(err, "fetching current state")
		}
	}

	behind := state.Time.Sub(timestamp)
	// Sequence unit depends on replication interval (minute, hour, day).
	return state.Sequence - int(math.Ceil(behind.Minutes()/interval.Minutes())), nil
}
