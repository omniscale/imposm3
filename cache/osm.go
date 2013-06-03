package cache

import (
	"bytes"
	bin "encoding/binary"
	"errors"
	"github.com/jmhodges/levigo"
	"os"
	"path/filepath"
	"strconv"
)

var levelDbWriteBufferSize, levelDbWriteBlockSize int64
var deltaCacheBunchSize int64

func init() {
	levelDbWriteBufferSize, _ = strconv.ParseInt(
		os.Getenv("GOPOSM_LEVELDB_BUFFERSIZE"), 10, 32)
	levelDbWriteBlockSize, _ = strconv.ParseInt(
		os.Getenv("GOPOSM_LEVELDB_BLOCKSIZE"), 10, 32)

	// bunchSize defines how many coordinates should be stored in a
	// single record. This is the maximum and a bunch will typically contain
	// less coordinates (e.g. when nodes are removes).
	//
	// A higher number improves -read mode (writing the cache) but also
	// increases the overhead during -write mode (reading coords).
	deltaCacheBunchSize, _ = strconv.ParseInt(
		os.Getenv("GOPOSM_DELTACACHE_BUNCHSIZE"), 10, 32)

	if deltaCacheBunchSize == 0 {
		deltaCacheBunchSize = 128
	}
}

var (
	NotFound = errors.New("not found")
)

type OSMCache struct {
	Dir          string
	Coords       *DeltaCoordsCache
	Ways         *WaysCache
	Nodes        *NodesCache
	Relations    *RelationsCache
	InsertedWays *InsertedWaysCache
	opened       bool
}

func (c *OSMCache) Close() {
	if c.Coords != nil {
		c.Coords.Close()
		c.Coords = nil
	}
	if c.Nodes != nil {
		c.Nodes.Close()
		c.Nodes = nil
	}
	if c.Ways != nil {
		c.Ways.Close()
		c.Ways = nil
	}
	if c.Relations != nil {
		c.Relations.Close()
		c.Relations = nil
	}
}

func NewOSMCache(dir string) *OSMCache {
	cache := &OSMCache{Dir: dir}
	return cache
}

func (c *OSMCache) Open() error {
	err := os.MkdirAll(c.Dir, 0755)
	if err != nil {
		return err
	}
	c.Coords, err = NewDeltaCoordsCache(filepath.Join(c.Dir, "coords"))
	if err != nil {
		return err
	}
	c.Nodes, err = NewNodesCache(filepath.Join(c.Dir, "nodes"))
	if err != nil {
		c.Close()
		return err
	}
	c.Ways, err = NewWaysCache(filepath.Join(c.Dir, "ways"))
	if err != nil {
		c.Close()
		return err
	}
	c.Relations, err = NewRelationsCache(filepath.Join(c.Dir, "relations"))
	if err != nil {
		c.Close()
		return err
	}
	c.InsertedWays, err = NewInsertedWaysCache(filepath.Join(c.Dir, "inserted_ways"))
	if err != nil {
		c.Close()
		return err
	}
	c.opened = true
	return nil
}

func (c *OSMCache) Exists() bool {
	if c.opened {
		return true
	}
	if _, err := os.Stat(filepath.Join(c.Dir, "coords")); !os.IsNotExist(err) {
		return true
	}
	if _, err := os.Stat(filepath.Join(c.Dir, "nodes")); !os.IsNotExist(err) {
		return true
	}
	if _, err := os.Stat(filepath.Join(c.Dir, "ways")); !os.IsNotExist(err) {
		return true
	}
	if _, err := os.Stat(filepath.Join(c.Dir, "relations")); !os.IsNotExist(err) {
		return true
	}
	if _, err := os.Stat(filepath.Join(c.Dir, "inserted_ways")); !os.IsNotExist(err) {
		return true
	}
	return false
}

func (c *OSMCache) Remove() error {
	if c.opened {
		c.Close()
	}
	if err := os.RemoveAll(filepath.Join(c.Dir, "coords")); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(c.Dir, "nodes")); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(c.Dir, "ways")); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(c.Dir, "relations")); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(c.Dir, "inserted_ways")); err != nil {
		return err
	}
	return nil
}

type Cache struct {
	db *levigo.DB
	wo *levigo.WriteOptions
	ro *levigo.ReadOptions
}

func (c *Cache) open(path string) error {
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(1024 * 1024 * 8))
	opts.SetCreateIfMissing(true)
	opts.SetMaxOpenFiles(64)
	// save a few bytes by allowing leveldb to use delta enconding
	// for up to n keys (instead of only 16)
	opts.SetBlockRestartInterval(128)
	if levelDbWriteBufferSize != 0 {
		opts.SetWriteBufferSize(int(levelDbWriteBufferSize))
	}
	if levelDbWriteBlockSize != 0 {
		opts.SetBlockSize(int(levelDbWriteBlockSize))
	}
	db, err := levigo.Open(path, opts)
	if err != nil {
		return err
	}
	c.db = db
	c.wo = levigo.NewWriteOptions()
	c.ro = levigo.NewReadOptions()
	return nil
}

func idToKeyBuf(id int64) []byte {
	b := make([]byte, 0, 8)
	buf := bytes.NewBuffer(b)
	bin.Write(buf, bin.BigEndian, &id)
	return b[:8]
}

func (p *Cache) Close() {
	p.db.Close()
}
