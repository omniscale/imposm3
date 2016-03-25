package cache

import (
	bin "encoding/binary"
	"errors"
	"os"
	"path/filepath"

	"github.com/jmhodges/levigo"
	"github.com/omniscale/imposm3/element"
)

var (
	NotFound = errors.New("not found")
)

const SKIP int64 = -1

type OSMCache struct {
	dir          string
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
	if c.InsertedWays != nil {
		c.InsertedWays.Close()
		c.InsertedWays = nil
	}
}

func NewOSMCache(dir string) *OSMCache {
	cache := &OSMCache{dir: dir}
	return cache
}

func (c *OSMCache) Open() error {
	err := os.MkdirAll(c.dir, 0755)
	if err != nil {
		return err
	}
	c.Coords, err = newDeltaCoordsCache(filepath.Join(c.dir, "coords"))
	if err != nil {
		return err
	}
	c.Nodes, err = newNodesCache(filepath.Join(c.dir, "nodes"))
	if err != nil {
		c.Close()
		return err
	}
	c.Ways, err = newWaysCache(filepath.Join(c.dir, "ways"))
	if err != nil {
		c.Close()
		return err
	}
	c.Relations, err = newRelationsCache(filepath.Join(c.dir, "relations"))
	if err != nil {
		c.Close()
		return err
	}
	c.InsertedWays, err = newInsertedWaysCache(filepath.Join(c.dir, "inserted_ways"))
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
	if _, err := os.Stat(filepath.Join(c.dir, "coords")); !os.IsNotExist(err) {
		return true
	}
	if _, err := os.Stat(filepath.Join(c.dir, "nodes")); !os.IsNotExist(err) {
		return true
	}
	if _, err := os.Stat(filepath.Join(c.dir, "ways")); !os.IsNotExist(err) {
		return true
	}
	if _, err := os.Stat(filepath.Join(c.dir, "relations")); !os.IsNotExist(err) {
		return true
	}
	if _, err := os.Stat(filepath.Join(c.dir, "inserted_ways")); !os.IsNotExist(err) {
		return true
	}
	return false
}

func (c *OSMCache) Remove() error {
	if c.opened {
		c.Close()
	}
	if err := os.RemoveAll(filepath.Join(c.dir, "coords")); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(c.dir, "nodes")); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(c.dir, "ways")); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(c.dir, "relations")); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(c.dir, "inserted_ways")); err != nil {
		return err
	}
	return nil
}

// FirstMemberIsCached checks whether the first way or node member is cached.
// Also returns true if there are no members of type WAY or NODE.
func (c *OSMCache) FirstMemberIsCached(members []element.Member) (bool, error) {
	for _, m := range members {
		if m.Type == element.WAY {
			_, err := c.Ways.GetWay(m.Id)
			if err == NotFound {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			return true, nil
		} else if m.Type == element.NODE {
			_, err := c.Coords.GetCoord(m.Id)
			if err == NotFound {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			return true, nil
		}
	}
	return true, nil
}

type cache struct {
	db      *levigo.DB
	options *cacheOptions
	cache   *levigo.Cache
	wo      *levigo.WriteOptions
	ro      *levigo.ReadOptions
}

func (c *cache) open(path string) error {
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
		opts.SetBlockSize(c.options.BlockSizeK * 1024)
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

func (c *cache) Close() {
	if c.ro != nil {
		c.ro.Close()
		c.ro = nil
	}
	if c.wo != nil {
		c.wo.Close()
		c.wo = nil
	}
	if c.db != nil {
		c.db.Close()
		c.db = nil
	}
	if c.cache != nil {
		c.cache.Close()
		c.cache = nil
	}

}
