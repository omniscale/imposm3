package expire

import (
	"bytes"
	"testing"

	"github.com/omniscale/imposm3/geom/geojson"
)

func TestWriteTiles(t *testing.T) {
	expected := "8627/5753/14\n"
	point := geojson.Point{1065162.58495039, 5965498.83778885}

	expireor := NewTileExpireor(14)
	expireor.Expire(point.Long, point.Lat)

	buf := new(bytes.Buffer)
	expireor.WriteTiles(buf)

	if buf.String() != expected {
		t.Error("Unexpected tiles were written", buf.String())
	}
}

type tilePair struct {
	in  Tile
	out Tile
}

var parentTests = []tilePair{
	{Tile{5, 10, 10}, Tile{2, 5, 9}},
	{Tile{486, 332, 10}, Tile{243, 166, 9}},
}

func TestParent(t *testing.T) {
	for _, tp := range parentTests {
		tile := tp.in
		parent := tile.Parent()

		if parent != tp.out {
			t.Error("Wrong parent ", parent, " did not match ", tp.out)
		}

	}
}
