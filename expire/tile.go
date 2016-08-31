package expire

import (
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/geom/geojson"
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
	linestring := geojson.LineString{}
	for _, n := range nodes {
		linestring = append(linestring, reproject(n.Long, n.Lat))
	}

	tiles, _ := CoverLinestring(linestring, te.maxZoom)
	te.tileAccess.Lock()
	te.tiles.MergeTiles(tiles)
	te.tileAccess.Unlock()
}

// Reproject from spherical mercator https://epsg.io/3857 to  http://epsg.io/4326
func reproject(lon, lat float64) geojson.Point {
	return geojson.Point{
		Long: lon * 180 / 20037508.34,
		Lat:  math.Atan(math.Exp(lat*math.Pi/20037508.34))*360/math.Pi - 90,
	}
}

func (te *TileExpireor) ExpirePolygon(nodes []element.Node) {
	outerRing := geojson.LineString{}
	for _, n := range nodes {
		outerRing = append(outerRing, reproject(n.Long, n.Lat))
	}
	poly := geojson.Polygon{outerRing}
	tiles := CoverPolygon(poly, te.maxZoom)

	te.tileAccess.Lock()
	te.tiles.MergeTiles(tiles)
	te.tileAccess.Unlock()
}

func (te *TileExpireor) Expire(lon, lat float64) {
	point := reproject(lon, lat)
	tile := CoverPoint(point, te.maxZoom)

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

func (t Tile) Parent() Tile {
	// top left
	if t.X%2 == 0 && t.Y%2 == 0 {
		return Tile{t.X / 2, t.Y / 2, t.Z - 1}
	}
	// bottom left
	if (t.X%2 == 0) && !(t.Y%2 == 0) {
		return Tile{t.X / 2, (t.Y - 1) / 2, t.Z - 1}
	}
	// top right
	if !(t.X%2 == 0) && (t.Y%2 == 0) {
		return Tile{(t.X - 1) / 2, (t.Y) / 2, t.Z - 1}
	}
	// bottom right
	return Tile{(t.X - 1) / 2, (t.Y - 1) / 2, t.Z - 1}
}
