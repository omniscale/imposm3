package import_

import (
	"strings"
	"testing"
	"time"
)

func TestFromPBF(t *testing.T) {
	expectedPBFTime, err := time.Parse(time.RFC3339, "2015-04-27T22:21:02+02:00")
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		name string
		// The estimated sequence changes as the PBF becomes older, because
		// sequences are not counted upwards during maintenance on
		// planet.openstreetmap.org. Check for range.
		minSeq, maxSeq int
		before         time.Duration
		interval       time.Duration
		url            string
		errContains    string
	}{
		{
			name:     "minutely defaults",
			minSeq:   1350000,
			maxSeq:   1368233,
			interval: time.Minute,
		},
		{
			name:     "minutely before 5d",
			minSeq:   1345000,
			maxSeq:   1361033,
			before:   time.Hour * 24 * 5,
			interval: time.Minute,
		},
		{
			name:     "hourly before 10h",
			minSeq:   22571,
			maxSeq:   22971,
			url:      "https://planet.openstreetmap.org/replication/hour/",
			before:   time.Hour * 10,
			interval: time.Hour,
		},
		{
			name:     "daily before 3d",
			minSeq:   950,
			maxSeq:   958,
			url:      "https://planet.openstreetmap.org/replication/day/",
			before:   time.Hour * 24 * 3,
			interval: time.Hour * 24,
		},
		{
			name:        "unable to fetch current state",
			url:         "https://unknownurl_planet.openstreetmap.org/replication/day/",
			before:      time.Hour * 24 * 3,
			interval:    time.Hour * 24,
			errContains: "No address associated with hostname",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			state, err := estimateFromPBF("../vendor/github.com/omniscale/go-osm/parser/pbf/monaco-20150428.osm.pbf", tt.before, tt.url, tt.interval)
			if tt.errContains != "" {
				if err == nil {
					t.Errorf("expected error with %q, got nil", tt.errContains)
				} else if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("expected error with %q, got %s", tt.errContains, err)
				}
				return
			}

			if err != nil {
				t.Fatal(err)
			}
			if tt.url == "" {
				if state.URL != "https://planet.openstreetmap.org/replication/minute/" {
					t.Error("unexpected state URL", state)
				}
			} else if state.URL != tt.url {
				t.Error("unexpected state URL", state)
			}
			if state.Sequence > tt.maxSeq || state.Sequence < tt.minSeq {
				// sequence is only estimated
				t.Error("unexpected sequence", state)
			}
			if !state.Time.Equal(expectedPBFTime) {
				t.Error("unexpected timestamp", state)
			}
		})
	}

}
