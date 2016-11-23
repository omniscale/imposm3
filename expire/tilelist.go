package expire

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/proj"
)

var mercBbox = [4]float64{
	-20037508.342789244,
	-20037508.342789244,
	20037508.342789244,
	20037508.342789244,
}

var mercRes [20]float64

func init() {
	res := 2 * 20037508.342789244 / 256

	for i, _ := range mercRes {
		mercRes[i] = res
		res /= 2
	}
}

// fraction of a tile that is added as a padding around an expired tile
const tilePadding = 0.1

func TileCoords(long, lat float64, zoom uint32) []tileKey {
	x, y := proj.WgsToMerc(long, lat)
	res := mercRes[zoom]
	x = x - mercBbox[0]
	y = mercBbox[3] - y
	tileX := float32(x / (res * 256))
	tileY := float32(y / (res * 256))

	tiles := make([]tileKey, 0, 4)
	for x := uint32(tileX - tilePadding); x <= uint32(tileX+tilePadding); x++ {
		for y := uint32(tileY - tilePadding); y <= uint32(tileY+tilePadding); y++ {
			tiles = append(tiles, tileKey{x, y})
		}
	}
	return tiles
}

type TileList struct {
	mu    sync.Mutex
	tiles map[tileKey]struct{}

	zoom uint32
	out  string
}

type tileKey struct {
	X uint32
	Y uint32
}

type tile struct {
	x uint32
	y uint32
	z uint32
}

func NewTileList(zoom int, out string) *TileList {
	return &TileList{
		tiles: make(map[tileKey]struct{}),
		zoom:  uint32(zoom),
		mu:    sync.Mutex{},
		out:   out,
	}
}

func (tl *TileList) addCoord(long, lat float64) {
	tl.mu.Lock()
	for _, t := range TileCoords(long, lat, tl.zoom) {
		tl.tiles[t] = struct{}{}
	}
	tl.mu.Unlock()
}

func (tl *TileList) Expire(long, lat float64) {
	tl.addCoord(long, lat)
}

func (tl *TileList) ExpireNodes(nodes []element.Node, closed bool) {
	for _, nd := range nodes {
		tl.addCoord(nd.Long, nd.Lat)
	}
}

func (tl *TileList) writeTiles(w io.Writer) error {
	for tileKey, _ := range tl.tiles {
		_, err := fmt.Fprintf(w, "%d/%d/%d\n", tl.zoom, tileKey.X, tileKey.Y)
		if err != nil {
			return err
		}
	}
	return nil
}

func (tl *TileList) Flush() error {
	if len(tl.tiles) == 0 {
		return nil
	}

	now := time.Now().UTC()
	dir := filepath.Join(tl.out, now.Format("20060102"))
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}
	fileName := filepath.Join(dir, now.Format("150405.000")+".tiles~")
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	err = tl.writeTiles(f)
	f.Close()
	if err != nil {
		return err
	}
	// wrote to .tiles~ and now atomically move file to .tiles
	return os.Rename(fileName, fileName[0:len(fileName)-1])
}
