package cache

import (
	bin "encoding/binary"
	"errors"
	"github.com/jmhodges/levigo"
	"os"
	"path/filepath"
)

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
	db      *levigo.DB
	options *CacheOptions
	cache   *levigo.Cache
	wo      *levigo.WriteOptions
	ro      *levigo.ReadOptions
}

func (c *Cache) open(path string) error {
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	if c.options.CacheSizeM > 0 {
		c.cache = levigo.NewLRUCache(c.options.CacheSizeM * 1024 * 1024)
		opts.SetCache(c.cache)
	}
	if c.options.MaxOpenFiles > 0 {
		opts.SetMaxOpenFiles(c.options.MaxOpenFiles)
	}
	if c.options.BlockRestartInterval > 0 {
		opts.SetBlockRestartInterval(c.options.BlockRestartInterval)
	}
	if c.options.WriteBufferSizeM > 0 {
		opts.SetWriteBufferSize(c.options.WriteBufferSizeM * 1024 * 1024)
	}
	if c.options.BlockSizeK > 0 {
		opts.SetBlockSize(c.options.BlockSizeK * 1024 * 1024)
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
	b := make([]byte, 8)
	bin.BigEndian.PutUint64(b, uint64(id))
	return b[:8]
}

func idFromKeyBuf(buf []byte) int64 {
	return int64(bin.BigEndian.Uint64(buf))
}

func (c *Cache) Close() {
	c.db.Close()
	if c.cache != nil {
		c.cache.Close()
	}
}
