package expire

import (
	"bytes"
	"testing"

	"github.com/omniscale/imposm3/element"
)

func TestWriteTiles(t *testing.T) {
	expected := "8593/5747/14\n8593/5748/14\n8593/5749/14\n"
	linestring := []element.Node{
		element.Node{Long: 8.826313018798828, Lat: 47.22796198584928},
		element.Node{Long: 8.82596969604492, Lat: 47.20755789924751},
		element.Node{Long: 8.826141357421875, Lat: 47.194845099780174},
	}

	expireor := NewTileExpireor(14)
	expireor.ExpireLinestring(linestring)

	buf := new(bytes.Buffer)
	expireor.WriteTiles(buf)

	if buf.String() != expected {
		t.Error("Unexpected tiles were written", buf.String())
	}
}
