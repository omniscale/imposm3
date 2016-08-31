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

func TestCalculateParents(t *testing.T) {
	th := TileHash{}
	tile := Tile{486, 332, 10}
	th.AddTile(tile)

	if len(th) != 1 {
		t.Error("Length of TileHash should be 1 after adding a tile")
	}

	th.CalculateParents()

	if len(th) != 11 {
		t.Error("Length of TileHash should be 11 after calculating all parents, it is ", len(th))
	}

}
