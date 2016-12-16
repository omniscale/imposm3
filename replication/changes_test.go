package replication

import (
	"time"

	"testing"
)

func TestParseYamlState(t *testing.T) {
	state, err := parseYamlState([]byte(`---
last_run: 2016-12-07 19:16:01.500000000 +00:00
sequence: 2139110
`))
	if err != nil {
		t.Fatal(err)
	}
	if state.Sequence != 2139110 {
		t.Error("unexpected sequence", state)
	}

	expected := time.Date(2016, 12, 07, 19, 16, 01, 500000000, time.UTC)
	if !state.Time.Time.Equal(expected) {
		t.Error("unexpected time", state)
	}

}
