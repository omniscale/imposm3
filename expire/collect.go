package expire

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"goposm/element"
	"io"
	"math"
	"sort"
	"sync"
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

func mercTileCoord(x, y float64, zoom uint32) (uint32, uint32) {
	res := mercRes[zoom]
	x = x - mercBbox[0]
	y = mercBbox[3] - y
	tileX := uint32(math.Floor(x / (res * 256)))
	tileY := uint32(math.Floor(y / (res * 256)))

	return tileX, tileY
}

type Tiles struct {
	tiles map[tileKey]bool
	zoom  uint32
	mu    *sync.Mutex
}

type tileKey struct {
	x uint32
	y uint32
}

type tile struct {
	x uint32
	y uint32
	z uint32
	d uint32
}

func NewTiles(zoom uint32) *Tiles {
	return &Tiles{make(map[tileKey]bool), zoom, &sync.Mutex{}}
}

func (tc *Tiles) addCoord(x, y float64) {
	tileX, tileY := mercTileCoord(x, y, tc.zoom)
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.tiles[tileKey{tileX, tileY}] = true
}

func (tc *Tiles) SortedTiles() []tile {
	tiles := make([]tile, len(tc.tiles))
	i := 0
	for tileKey, _ := range tc.tiles {
		tiles[i] = tile{
			tileKey.x,
			tileKey.y,
			tc.zoom,
			hilbert(tileKey.x, tileKey.y, tc.zoom),
		}
		i++
	}
	sort.Sort(byHilbert(tiles))
	return tiles
}

func (tc *Tiles) ExpireFromNodes(nodes []element.Node) {
	for _, nd := range nodes {
		tc.addCoord(nd.Long, nd.Lat)
	}
}

type byHilbert []tile

func (h byHilbert) Len() int           { return len(h) }
func (h byHilbert) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h byHilbert) Less(i, j int) bool { return h[i].d < h[j].d }

// Hilbert returns the distance of tile x, y in a hilbert curve of
// level z, where z=0 is 1x1, z=1 is 2x2, etc.
func hilbert(x, y, z uint32) uint32 {
	n := uint32(2 << (z - 1))
	var rx, ry, d, s uint32
	for s = n / 2; s > 0; s /= 2 {
		if (x & s) > 0 {
			rx = 1
		} else {
			rx = 0
		}
		if (y & s) > 0 {
			ry = 1
		} else {
			ry = 0
		}
		d += s * s * ((3 * rx) ^ ry)
		x, y = rot(s, x, y, rx, ry)
	}
	return d
}

//rotate/flip a quadrant appropriately
func rot(n, x, y, rx, ry uint32) (uint32, uint32) {
	if ry == 0 {
		if rx == 1 {
			x = n - 1 - x
			y = n - 1 - y
		}

		//Swap x and y
		return y, x
	}
	return x, y
}

func WriteTileExpireList(tiles []tile, writer io.Writer) error {
	for _, tile := range tiles {
		_, err := fmt.Fprintf(writer, "%d/%d/%d\n", tile.z, tile.x, tile.y)
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteTileExpireDb(tiles []tile, dbfile string) error {
	db, err := sql.Open("sqlite3", dbfile)
	if err != nil {
		return err
	}
	defer db.Close()

	stmts := []string{
		`create table if not exists tiles (
            x integer,
            y integer,
            z integer,
            time datetime,
            primary key (x, y, z)
        )`,
	}
	for _, stmt := range stmts {
		_, err := db.Exec(stmt)
		if err != nil {
			return err
		}
	}

	stmt, err := db.Prepare(`insert or replace into tiles (x, y, z, time) values (?, ?, ?, DATETIME('now'))`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, tile := range tiles {
		_, err := stmt.Exec(tile.x, tile.y, tile.z)
		if err != nil {
			return err
		}
	}
	return nil
}
