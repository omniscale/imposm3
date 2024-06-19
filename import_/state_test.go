package import_

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestStateFromTimestamp(t *testing.T) {
	for _, tt := range []struct {
		name string
		// We estimate the initial sequence by getting the current sequence
		// from planet.openstreetmap.org and then counting backwards. However,
		// we will count to far (lower then actual seq) as some sequences will
		// contain more then one minute, hour or day (due to maintenance or
		// high load).
		// We handle this by testing for a range of possible sequences
		// (assuming a certain drift over time).
		maxSeq             int     // should be the actual seq from planet.osm.org
		minSeq             int     // calculated from expectedDailyDrift
		expectedDailyDrift float64 // how many sequences do we expect to get skipped per day?

		timestamp string
		before    time.Duration
		interval  time.Duration
		url       string
		errMatch  string
	}{
		{
			name:               "minutely defaults",
			timestamp:          "2024-01-01T01:00:00+00:00",
			expectedDailyDrift: 45,
			maxSeq:             5900209,
			interval:           time.Minute,
		},
		{
			name:               "hourly before 10h",
			timestamp:          "2024-01-01T10:00:00+00:00",
			expectedDailyDrift: 0.1,
			maxSeq:             99065,
			before:             time.Hour * 10,
			interval:           time.Hour,
		},
		{
			name:               "hourly before 240h",
			timestamp:          "2024-01-01T00:00:00+00:00",
			expectedDailyDrift: 0.1,
			maxSeq:             98825,
			before:             time.Hour * 24 * 10,
			interval:           time.Hour,
		},
		{
			name:               "daily before 3d",
			timestamp:          "2015-04-27T22:21:02+02:00",
			expectedDailyDrift: 1.0 / 365,
			maxSeq:             958,
			before:             time.Hour * 24 * 3,
			interval:           time.Hour * 24,
		},
		{
			name:      "unable to fetch current state",
			timestamp: "2015-04-27T22:21:02+02:00",
			url:       "https://unknownurl_planet.openstreetmap.org/replication/day/",
			before:    time.Hour * 24 * 3,
			interval:  time.Hour * 24,
			errMatch:  "no such host|No address associated with hostname",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			timestamp, err := time.Parse(time.RFC3339, tt.timestamp)
			if err != nil {
				t.Fatal(err)
			}

			state, err := estimateFromTimestamp(timestamp, tt.before, tt.url, tt.interval)
			if tt.errMatch != "" {
				if err == nil {
					t.Errorf("expected error with %q, got nil", tt.errMatch)
				} else if ok, merr := regexp.MatchString(tt.errMatch, err.Error()); !ok || merr != nil {
					t.Errorf("expected error with %q, got %s", tt.errMatch, err)
				}
				return
			}

			if tt.minSeq == 0 {
				tt.minSeq = tt.maxSeq - int(time.Since(timestamp).Hours()/24*tt.expectedDailyDrift)
			}

			if err != nil {
				t.Fatal(err)
			}
			if tt.url == "" {
				if !strings.HasPrefix(state.URL, "https://planet.openstreetmap.org/replication/") {
					t.Error("unexpected state URL", state)
				}
			} else if state.URL != tt.url {
				t.Error("unexpected state URL", state)
			}
			if state.Sequence > tt.maxSeq || state.Sequence < tt.minSeq {
				// sequence is only estimated
				t.Errorf("unexpected sequence, expected: %d < %d < %d", tt.minSeq, state.Sequence, tt.maxSeq)
			}
			if !state.Time.Equal(timestamp) {
				t.Error("unexpected timestamp", state)
			}
		})
	}

}

func TestFromPBF(t *testing.T) {
	expectedPBFTime, err := time.Parse(time.RFC3339, "2015-04-27T22:21:02+02:00")
	if err != nil {
		t.Fatal(err)
	}

	state, err := estimateFromPBF("../vendor/github.com/omniscale/go-osm/parser/pbf/monaco-20150428.osm.pbf", time.Hour*24*3, "https://planet.openstreetmap.org/replication/day/", time.Hour*24)
	if err != nil {
		t.Fatal(err)
	}
	if state.URL != "https://planet.openstreetmap.org/replication/day/" {
		t.Error("unexpected state URL", state)
	}
	// The estimated sequence changes as the PBF becomes older, because
	// sequences are not counted upwards during maintenance on
	// planet.openstreetmap.org. Check for range.
	if state.Sequence > 958 || state.Sequence < 950 {
		// sequence is only estimated
		t.Error("unexpected sequence", state)
	}
	if !state.Time.Equal(expectedPBFTime) {
		t.Error("unexpected timestamp", state)
	}
}
