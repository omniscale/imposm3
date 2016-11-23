package expire

import (
	"testing"
)

func TestTileCoords(t *testing.T) {
	tests := []struct {
		long  float64
		lat   float64
		zoom  uint32
		tiles []tileKey
	}{
		{0, 0, 14, []tileKey{{8191, 8191}, {8191, 8192}, {8192, 8191}, {8192, 8192}}},
		{0.01, 0, 14, []tileKey{{8192, 8191}, {8192, 8192}}},
		{0, 0.01, 14, []tileKey{{8191, 8191}, {8192, 8191}}},
		{0.01, 0.01, 14, []tileKey{{8192, 8191}}},
		{0.02, 0.01, 14, []tileKey{{8192, 8191}, {8193, 8191}}},
	}

	for _, test := range tests {
		actual := TileCoords(test.long, test.lat, test.zoom)
		if len(actual) != len(test.tiles) {
			t.Errorf("%v != %v", actual, test.tiles)
			continue
		}
		for i := range actual {
			if actual[i] != test.tiles[i] {
				t.Errorf("%v != %v", actual, test.tiles)
			}
		}
	}
}
