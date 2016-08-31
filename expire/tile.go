package expire

import (
	"fmt"
	"io"

	"github.com/omniscale/imposm3/element"
)

type Tile struct {
	X int
	Y int
	Z int
}

func NewTile(x, y float64, z int) Tile {
	return Tile{X: int(x), Y: int(y), Z: z}
}

type ByID []Tile

func (t ByID) Len() int           { return len(t) }
func (t ByID) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t ByID) Less(i, j int) bool { return t[i].toID() < t[j].toID() }

type TileFraction struct {
	X float64
	Y float64
}

type ByYX []TileFraction

func (t ByYX) Len() int      { return len(t) }
func (t ByYX) Swap(i, j int) { t[i], t[j] = t[j], t[i] }
func (t ByYX) Less(i, j int) bool {
	return t[i].Y < t[j].Y || (t[i].Y == t[j].Y && t[i].X < t[j].X)
}

// The tile expireor keeps a list of dirty XYZ tiles
// that are covered by the expired polygons, linestring and points
func NewTileExpireor(maxZoom int) TileExpireor {
	return TileExpireor{
		tiles:   make(TileHash),
		maxZoom: maxZoom,
	}
}

type TileExpireor struct {
	// Space efficient tile store
	tiles TileHash
	// Max zoom level to evaluate
	maxZoom int
}

func (te *TileExpireor) ExpireLinestring(nodes []element.Node) {
	linestring := []Point{}
	//TODO: If we would not have Point we could save this conversion
	for _, node := range nodes {
		linestring = append(linestring, Point{node.Long, node.Lat})
	}

	tiles, _ := CoverLinestring(linestring, te.maxZoom)
	te.tiles.MergeTiles(tiles)
}

func (te *TileExpireor) ExpirePolygon(nodes []element.Node) {
	linearRing := []Point{}
	//TODO: If we would not have Point we could save this conversion
	for _, node := range nodes {
		linearRing = append(linearRing, Point{node.Long, node.Lat})
	}

	tiles := CoverPolygon(linearRing, te.maxZoom)
	te.tiles.MergeTiles(tiles)
}

func (te *TileExpireor) Expire(lon, lat float64) {
	tile := CoverPoint(lon, lat, te.maxZoom)
	te.tiles.AddTile(tile)
}

func (te *TileExpireor) WriteTiles(w io.Writer) {
	for id, _ := range te.tiles {
		tile := fromID(id)
		fmt.Fprintf(w, "%d/%d/%d\n", tile.X, tile.Y, tile.Z)
	}
}
