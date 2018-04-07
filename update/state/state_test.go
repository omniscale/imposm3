package state

import (
	"testing"
	"time"
)

func TestFromPBF(t *testing.T) {
	state, err := FromPbf("../../parser/pbf/monaco-20150428.osm.pbf", time.Hour*1, "", time.Minute*1)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := time.Parse(time.RFC3339, "2015-04-27T22:21:02+02:00")
	if err != nil {
		t.Fatal(err)
	}
	if state.Sequence > 1368233 || state.Sequence < 1360000 {
		// sequence is only estimated
		t.Error("unexpected sequence", state)
	}
	if !state.Time.Equal(expected) {
		t.Error("unexpected timestamp", state)
	}
}
