package expire

import "testing"

func TestMarkDirtyTile(t *testing.T) {
	te := TileExpireor{tiles: make(map[string]Tile)}
	tile := Tile{8593, 5747, 14}
	te.MarkDirtyTile(tile)
	if te.tiles["8593/5747/14"] != tile {
		t.Error("Expected tile to be marked dirty", te.tiles["8593/5747/14"])
	}
}
