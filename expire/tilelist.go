package expire

import (
	"fmt"
	"io"
	"math"
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

func tileCoord(long, lat float64, zoom int) (float64, float64) {
	x, y := proj.WgsToMerc(long, lat)
	res := mercRes[zoom]
	x = x - mercBbox[0]
	y = mercBbox[3] - y
	tileX := float64(x / (res * 256))
	tileY := float64(y / (res * 256))
	return tileX, tileY
}

type TileList struct {
	mu    sync.Mutex
	tiles map[tileKey]struct{}

	zoom int
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
		zoom:  zoom,
		mu:    sync.Mutex{},
		out:   out,
	}
}

func (tl *TileList) Expire(long, lat float64) {
	tl.addCoord(long, lat)
}

func (tl *TileList) ExpireNodes(nodes []element.Node, closed bool) {
	if len(nodes) == 0 {
		return
	}
	if closed {
		box := nodesBbox(nodes)
		tiles := numBboxTiles(box, tl.zoom)
		if tiles > 500 {
			tl.expireLine(nodes)
		} else {
			tl.expireBox(box)
		}
	} else {
		tl.expireLine(nodes)
	}
}

// expire a single point. Point is padded by 0.2 tiles to expire nearby tiles
// for nodes at a tile border.
func (tl *TileList) addCoord(long, lat float64) {
	// fraction of a tile that is added as a padding around a single node
	const tilePadding = 0.2
	tl.mu.Lock()
	tileX, tileY := tileCoord(long, lat, tl.zoom)
	for x := uint32(tileX - tilePadding); x <= uint32(tileX+tilePadding); x++ {
		for y := uint32(tileY - tilePadding); y <= uint32(tileY+tilePadding); y++ {
			tl.tiles[tileKey{x, y}] = struct{}{}
		}
	}
	tl.mu.Unlock()
}

// expireLine expires all tiles that are intersected by the line segments
// between the nodes
func (tl *TileList) expireLine(nodes []element.Node) {
	if len(nodes) == 1 {
		tl.addCoord(nodes[0].Long, nodes[0].Lat)
		return
	}
	tl.mu.Lock()
	defer tl.mu.Unlock()
	for i := 0; i < len(nodes)-1; i++ {
		x1, y1 := tileCoord(nodes[i].Long, nodes[i].Lat, tl.zoom)
		x2, y2 := tileCoord(nodes[i+1].Long, nodes[i+1].Lat, tl.zoom)
		if int(x1) == int(x2) && int(y1) == int(y2) {
			tl.tiles[tileKey{X: uint32(x1), Y: uint32(y1)}] = struct{}{}
		} else {
			for _, tk := range bresenham(x1, y1, x2, y2) {
				tl.tiles[tk] = struct{}{}
			}
		}
	}
}

// expireBox expires all tiles inside the bbox
func (tl *TileList) expireBox(b bbox) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	x1, y1 := tileCoord(b.minx, b.maxy, tl.zoom)
	x2, y2 := tileCoord(b.maxx, b.miny, tl.zoom)
	for x := uint32(x1); x <= uint32(x2); x++ {
		for y := uint32(y1); y <= uint32(y2); y++ {
			tl.tiles[tileKey{x, y}] = struct{}{}
		}
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
	tl.mu.Lock()
	defer tl.mu.Unlock()
	if len(tl.tiles) == 0 {
		return nil
	}

	now := time.Now().UTC()
	dir := filepath.Join(tl.out, now.Format("20060102"))
	err := os.MkdirAll(dir, 0775)
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
	tl.tiles = make(map[tileKey]struct{})
	// wrote to .tiles~ and now atomically move file to .tiles
	return os.Rename(fileName, fileName[0:len(fileName)-1])
}

type bbox struct {
	minx, miny, maxx, maxy float64
}

func nodesBbox(nodes []element.Node) bbox {
	b := bbox{nodes[0].Long, nodes[0].Lat, nodes[0].Long, nodes[0].Lat}

	for i := 1; i < len(nodes); i++ {
		if b.maxx < nodes[i].Long {
			b.maxx = nodes[i].Long
		}
		if b.maxy < nodes[i].Lat {
			b.maxy = nodes[i].Lat
		}
		if b.minx > nodes[i].Long {
			b.minx = nodes[i].Long
		}
		if b.miny > nodes[i].Lat {
			b.miny = nodes[i].Lat
		}
	}
	return b
}

func numBboxTiles(b bbox, zoom int) int {
	x1, y1 := tileCoord(b.minx, b.maxy, zoom)
	x2, y2 := tileCoord(b.maxx, b.miny, zoom)
	return int(math.Abs((x2 - x1 + 1) * (y2 - y1 + 1)))
}

func bresenham(x1, y1, x2, y2 float64) []tileKey {
	tiles := make([]tileKey, 0, 4)
	steep := false
	dx := math.Abs(x2 - x1)
	sx := -1.0
	if (x2 - x1) > 0 {
		sx = 1.0
	}
	dy := math.Abs(y2 - y1)
	sy := -1.0
	if (y2 - y1) > 0 {
		sy = 1.0
	}

	if dy > dx {
		steep = true
		x1, y1 = y1, x1
		dx, dy = dy, dx
		sx, sy = sy, sx
	}

	e := 2*dy - dx
	for i := 0.0; i < dx; i++ {
		if steep {
			tiles = append(tiles, tileKey{X: uint32(y1), Y: uint32(x1)})
		} else {
			tiles = append(tiles, tileKey{X: uint32(x1), Y: uint32(y1)})
		}
		for e >= 0 {
			y1 += sy
			e -= 2 * dx
		}
		x1 += sx
		e += 2 * dy
	}
	tiles = append(tiles, tileKey{X: uint32(x2), Y: uint32(y2)})
	return tiles
}
