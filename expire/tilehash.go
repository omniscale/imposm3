package expire

import "sort"

type TileHash map[int]struct{}

func (th TileHash) AddTile(t Tile) {
	th[t.toID()] = struct{}{}
}

func (th TileHash) MergeTiles(other TileHash) {
	for id, _ := range other {
		th[id] = struct{}{}
	}
}

func (th TileHash) TileExists(t Tile) bool {
	_, ok := th[t.toID()]
	return ok
}

func FromTiles(tiles []Tile) TileHash {
	th := TileHash{}
	for _, t := range tiles {
		th.AddTile(t)
	}
	return th
}

func (th TileHash) ToTiles() []Tile {
	tiles := []Tile{}
	for id, _ := range th {
		tiles = append(tiles, fromID(id))
	}
	sort.Sort(ByID(tiles))
	return tiles
}

func fromID(id int) Tile {
	z := id % 32
	dim := 2 * (1 << uint(z))
	xy := ((id - z) / 32)
	x := xy % dim
	y := ((xy - x) / dim) % dim
	return Tile{x, y, z}
}

func (t Tile) toID() int {
	dim := 2 * (1 << uint(t.Z))
	return ((dim*t.Y + t.X) * 32) + t.Z
}

func (th TileHash) CalculateParents() {
	for id, _ := range th {
		tile := fromID(id)
		parent := tile
		for parent.Z > 0 {
			parent = parent.Parent()
			th.AddTile(parent)
		}
	}
}
