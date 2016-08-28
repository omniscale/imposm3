package expire

import "testing"

func comparePointTile(t *testing.T, lon, lat float64, x, y, z int) {
	expected := Tile{x, y, z}
	actual := PointToTile(lon, lat, z)
	if actual != expected {
		t.Error("Expected ", expected, ", got ", actual)
	}
}

func TestKey(t *testing.T) {
	tile := Tile{8593, 5747, 14}
	if tile.Key() != "8593/5747/14" {
		t.Error("Wrong key for tile", tile.Key())
	}
}

func TestPointToTile(t *testing.T) {
	// Knie's Kinderzoo in Rapperswil, Switzerland
	comparePointTile(t, 8.8223, 47.2233, 8593, 5747, 14)
}
