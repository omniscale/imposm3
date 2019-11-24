package cache

import (
	bin "encoding/binary"
	"errors"
	"os"
	"path/filepath"

	"github.com/omniscale/go-osm"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	NotFound = errors.New("not found")
)

const SKIP int64 = -1

type OSMCache struct {
	dir       string
	Coords    *DeltaCoordsCache
	Ways      *WaysCache
	Nodes     *NodesCache
	Relations *RelationsCache
	opened    bool
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
// Also returns true if there are no members of type WayMember or NodeMember.
func (c *OSMCache) FirstMemberIsCached(members []osm.Member) (bool, error) {
	for _, m := range members {
		if m.Type == osm.WayMember {
			_, err := c.Ways.GetWay(m.ID)
			if err == NotFound {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			return true, nil
		} else if m.Type == osm.NodeMember {
			_, err := c.Coords.GetCoord(m.ID)
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
	db      *leveldb.DB
	options *cacheOptions
	wo      *opt.WriteOptions
	ro      *opt.ReadOptions
}

func (c *cache) open(path string) error {
	opts := &opt.Options{}
	opts.ErrorIfMissing = false
	if c.options.CacheSizeM > 0 {
		opts.BlockCacheCapacity = c.options.CacheSizeM * 1024 * 1024
	}
	if c.options.MaxOpenFiles > 0 {
		opts.OpenFilesCacheCapacity = c.options.MaxOpenFiles
	}
	if c.options.BlockRestartInterval > 0 {
		opts.BlockRestartInterval = c.options.BlockRestartInterval
	}
	if c.options.WriteBufferSizeM > 0 {
		opts.WriteBuffer = c.options.WriteBufferSizeM * 1024 * 1024
	}
	if c.options.BlockSizeK > 0 {
		opts.BlockSize = c.options.BlockSizeK * 1024
	}
	//if c.options.MaxFileSizeM > 0 {
	//	// max file size option is only available with LevelDB 1.21 and higher
	//	// build with -tags="ldppost121" to enable this option.
	//	setMaxFileSize(opts, c.options.MaxFileSizeM*1024*1024)
	//}

	db, err := leveldb.OpenFile(path, opts)
	if err != nil {
		return err
	}
	c.db = db
	c.wo = &opt.WriteOptions{}
	c.ro = &opt.ReadOptions{}

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
		c.ro = nil
	}
	if c.wo != nil {
		c.wo = nil
	}
	if c.db != nil {
		c.db.Close()
		c.db = nil
	}
}
