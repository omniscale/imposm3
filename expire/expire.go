package expire

import (
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
