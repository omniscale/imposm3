package expire

import (
	"reflect"
	"testing"
)

func comparePointTile(t *testing.T, lon, lat float64, x, y, z int) {
	expected := Tile{x, y, z}
	actual := PointToTile(lon, lat, z)
	if actual != expected {
		t.Error("Expected ", expected, ", got ", actual)
	}
}

func TestPointToTile(t *testing.T) {
	// Knie's Kinderzoo in Rapperswil, Switzerland
	comparePointTile(t, 8.8223, 47.2233, 8593, 5747, 14)
}

func TestLongLinestringToTile(t *testing.T) {
	points := []Point{
		Point{-106.21719360351562, 28.592359801121567},
		Point{-106.1004638671875, 28.791130513231813},
		Point{-105.87661743164062, 28.864519767126602},
		Point{-105.82374572753905, 28.60743139267596},
	}
	tiles := LinestringToTile(points, 10)

	expectedTiles := []Tile{
		Tile{209, 427, 10},
		Tile{209, 426, 10},
		Tile{210, 426, 10},
		Tile{210, 427, 10},
	}

	if !reflect.DeepEqual(tiles, expectedTiles) {
		t.Error("Unexpected tiles", tiles)
	}
}

func TestLinestringToTile(t *testing.T) {
	points := []Point{
		Point{8.826313018798828, 47.22796198584928},
		Point{8.82596969604492, 47.20755789924751},
		Point{8.826141357421875, 47.194845099780174},
	}

	tiles := LinestringToTile(points, 14)
	expectedTiles := []Tile{
		Tile{8593, 5747, 14},
		Tile{8593, 5748, 14},
		Tile{8593, 5749, 14},
	}

	if !reflect.DeepEqual(tiles, expectedTiles) {
		t.Error("Unexpected tiles", tiles)
	}
}
