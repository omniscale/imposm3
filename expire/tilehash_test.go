package expire

import (
	"reflect"
	"testing"
)

func TestTileHash(t *testing.T) {
	expected := []Tile{
		Tile{209, 426, 10},
		Tile{210, 426, 10},
		Tile{209, 427, 10},
		Tile{210, 427, 10},
	}

	actual := FromTiles(expected).ToTiles()
	if !reflect.DeepEqual(expected, actual) {
		t.Error("Tiles before inserting in TileHash and afterwards are not the same", actual)
	}
}
