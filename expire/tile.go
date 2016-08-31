package expire

import (
	"fmt"
	"io"
	"math"
	"sync"

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
	// Allow writing to tile hash
	tileAccess sync.Mutex
}

func (te *TileExpireor) ExpireLinestring(nodes []element.Node) {
	linestring := []Point{}
	//TODO: If we would not have Point we could save this conversion
	for _, node := range nodes {
		linestring = append(linestring, ToPoint(node))
	}

	tiles, _ := CoverLinestring(linestring, te.maxZoom)
	te.tileAccess.Lock()
	te.tiles.MergeTiles(tiles)
	te.tileAccess.Unlock()
}

func Reproject(lon, lat float64) (float64, float64) {
	return lon * 180 / 20037508.34, math.Atan(math.Exp(lat*math.Pi/20037508.34))*360/math.Pi - 90
}

// TODO: Adapt algorithms to work directly with meters instead
// of reprojecting it. Still don't get the projections quite.
func ToPoint(n element.Node) Point {
	lon, lat := Reproject(n.Long, n.Lat)
	return Point{lon, lat}
}

func (te *TileExpireor) ExpirePolygon(nodes []element.Node) {
	linearRing := []Point{}
	//TODO: If we would not have Point we could save this conversion
	for _, node := range nodes {
		linearRing = append(linearRing, ToPoint(node))
	}

	tiles := CoverPolygon(linearRing, te.maxZoom)
	te.tileAccess.Lock()
	te.tiles.MergeTiles(tiles)
	te.tileAccess.Unlock()
}

func (te *TileExpireor) Expire(lon, lat float64) {
	lon, lat = Reproject(lon, lat)
	tile := CoverPoint(lon, lat, te.maxZoom)
	fmt.Println(lon, lat)
	te.tileAccess.Lock()
	te.tiles.AddTile(tile)
	te.tileAccess.Unlock()
}

func (te *TileExpireor) WriteTiles(w io.Writer) {
	for id, _ := range te.tiles {
		tile := fromID(id)
		fmt.Fprintf(w, "%d/%d/%d\n", tile.X, tile.Y, tile.Z)
	}
}
