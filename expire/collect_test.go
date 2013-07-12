package expire

import (
	"testing"
)

func TestMercTileCoord(t *testing.T) {
	if x, y := mercTileCoord(0, 0, 0); x != 0 || y != 0 {
		t.Fatal(x, y)
	}

	if x, y := mercTileCoord(0, 0, 1); x != 1 || y != 1 {
		t.Fatal(x, y)
	}

	if x, y := mercTileCoord(-10000, 10000, 1); x != 0 || y != 0 {
		t.Fatal(x, y)
	}

	if x, y := mercTileCoord(914785.4932536, 7010978.3787268, 18); x != 137055 || y != 85210 {
		t.Fatal(x, y)
	}
	if x, y := mercTileCoord(914785.4932536, 7010978.3787268, 14); x != 8565 || y != 5325 {
		t.Fatal(x, y)
	}

}

func TestHilbert(t *testing.T) {
	if d := hilbert(0, 0, 1); d != 0 {
		t.Fatal(d)
	}
	if d := hilbert(0, 1, 1); d != 1 {
		t.Fatal(d)
	}
	if d := hilbert(1, 1, 1); d != 2 {
		t.Fatal(d)
	}
	if d := hilbert(1, 0, 1); d != 3 {
		t.Fatal(d)
	}

	if d := hilbert(2, 0, 2); d != 14 {
		t.Fatal(d)
	}

	if d := hilbert(1, 3, 3); d != 12 {
		t.Fatal(d)
	}

}

func TestTileCollection(t *testing.T) {
	tc := NewTiles(14)

	tc.addCoord(914785.4932536, 7010978.3787268) // 8565 5325
	if len(tc.SortedTiles()) != 1 {
		t.Fatal(tc.tiles)
	}
	// add twice
	tc.addCoord(914785.4932536, 7010978.3787268)
	if len(tc.SortedTiles()) != 1 {
		t.Fatal(tc.tiles)
	}

	// different coord, but same tile
	tc.addCoord(914785.4932536, 7010778.3787268)
	if len(tc.SortedTiles()) != 1 {
		t.Fatal(tc.tiles)
	}

	tc.addCoord(915785.4932536, 7010778.3787268) // 8566 5325
	tc.addCoord(915785.4932536, 7020778.3787268) // 8566 5321
	tc.addCoord(915785.4932536, 7000778.3787268) // 8566 5329

	tc.addCoord(1915785.4932536, 7010778.3787268)  // 8975 5325
	tc.addCoord(1915785.4932536, 17010778.3787268) // 8975 1237
	if tiles := tc.SortedTiles(); len(tiles) != 6 ||
		tiles[0].x != 8566 ||
		tiles[1].x != 8565 ||
		tiles[2].x != 8566 ||
		tiles[3].x != 8566 ||
		tiles[4].x != 8975 ||
		tiles[5].x != 8975 {
		t.Fatal(tiles)
	}

}
