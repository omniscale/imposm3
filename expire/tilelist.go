package expire

import (
	"fmt"
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/proj"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
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

func TileCoord(long, lat float64, zoom uint32) (uint32, uint32) {
	x, y := proj.WgsToMerc(long, lat)
	res := mercRes[zoom]
	x = x - mercBbox[0]
	y = mercBbox[3] - y
	tileX := uint32(math.Floor(x / (res * 256)))
	tileY := uint32(math.Floor(y / (res * 256)))

	return tileX, tileY
}

type TileList struct {
	mu    sync.Mutex
	tiles map[tileKey]struct{}

	zoom uint32
	out  string
}

type tileKey struct {
	x uint32
	y uint32
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
	tileX, tileY := TileCoord(long, lat, tl.zoom)
	tl.mu.Lock()
	tl.tiles[tileKey{tileX, tileY}] = struct{}{}
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
		_, err := fmt.Fprintf(w, "%d/%d/%d\n", tl.zoom, tileKey.x, tileKey.y)
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
