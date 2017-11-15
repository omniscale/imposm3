package state

import (
	"testing"
	"time"
)

func TestFromPBF(t *testing.T) {
	state, err := FromPbf("../../parser/pbf/monaco-20150428.osm.pbf", time.Hour*1)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := time.Parse(time.RFC3339, "2015-04-27T22:21:02+02:00")
	if err != nil {
		t.Fatal(err)
	}
	if state.Sequence != 1368231 || !state.Time.Equal(expected) {
		t.Error(state)
	}
}
