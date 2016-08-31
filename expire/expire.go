package expire

import (
	"fmt"
	"io"

	"github.com/omniscale/imposm3/element"
)

type Expireor interface {
	Expire(long, lat float64)
}

func ExpireNodes(expireor Expireor, nodes []element.Node) {
	for _, nd := range nodes {
		expireor.Expire(nd.Long, nd.Lat)
	}
}

type TileExpireor struct {
	tiles map[int]Tile
}

func (te *TileExpireor) MarkDirtyTile(t Tile) {
	te.tiles[t.toID()] = t
}

func (te *TileExpireor) DirtyTiles() []Tile {
	// How big should start be
	tiles := make([]Tile, 64000)

	for _, tile := range te.tiles {
		tiles = append(tiles, tile)
	}
	return tiles
}

func (te *TileExpireor) Expire(lon, lat float64) {
	// TODO: Zoom level is hardcoded
	te.MarkDirtyTile(PointToTile(lon, lat, 14))
}

func (te *TileExpireor) WriteTiles(w io.Writer) {
	for id, _ := range te.tiles {
		tile := fromID(id)
		fmt.Fprintf(w, "%d/%d/%d\n", tile.X, tile.Y, tile.Z)
	}
}
