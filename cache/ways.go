package cache

import (
	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/cache/binary"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type WaysCache struct {
	cache
}

func newWaysCache(path string) (*WaysCache, error) {
	cache := WaysCache{}
	cache.options = &globalCacheOptions.Ways
	err := cache.open(path)
	if err != nil {
		return nil, err
	}
	return &cache, err
}

func (c *WaysCache) PutWay(way *osm.Way) error {
	if way.ID == SKIP {
		return nil
	}
	keyBuf := idToKeyBuf(way.ID)
	data, err := binary.MarshalWay(way)
	if err != nil {
		return err
	}
	return c.db.Put(keyBuf, data, c.wo)
}

func (c *WaysCache) PutWays(ways []osm.Way) error {
	batch := new(leveldb.Batch)

	for _, way := range ways {
		if way.ID == SKIP {
			continue
		}
		keyBuf := idToKeyBuf(way.ID)
		data, err := binary.MarshalWay(&way)
		if err != nil {
			return err
		}
		batch.Put(keyBuf, data)
	}
	return c.db.Write(batch, c.wo)
}

func (c *WaysCache) GetWay(id int64) (*osm.Way, error) {
	keyBuf := idToKeyBuf(id)
	data, err := c.db.Get(keyBuf, c.ro)
	if err != nil && err != leveldb.ErrNotFound {
		return nil, err
	}
	if data == nil {
		return nil, NotFound
	}
	way, err := binary.UnmarshalWay(data)
	if err != nil {
		return nil, err
	}
	way.ID = id
	return way, nil
}

func (c *WaysCache) DeleteWay(id int64) error {
	keyBuf := idToKeyBuf(id)
	return c.db.Delete(keyBuf, c.wo)
}

func (c *WaysCache) Iter() chan *osm.Way {
	ways := make(chan *osm.Way, 1024)
	go func() {
		ro := opt.ReadOptions{}
		ro.DontFillCache = true
		it := c.db.NewIterator(nil, &ro)
		// we need to Close the iter before closing the
		// chan (and thus signaling that we are done)
		// to avoid race where db is closed before the iterator
		defer close(ways)
		defer it.Release()
		it.First()
		for ; it.Valid(); it.Next() {
			way, err := binary.UnmarshalWay(it.Value())
			if err != nil {
				panic(err)
			}
			way.ID = idFromKeyBuf(it.Key())
			ways <- way
		}
	}()
	return ways
}

func (c *WaysCache) FillMembers(members []osm.Member) error {
	if members == nil || len(members) == 0 {
		return nil
	}
	for i, member := range members {
		if member.Type != osm.WayMember {
			continue
		}
		way, err := c.GetWay(member.ID)
		if err != nil {
			return err
		}
		members[i].Way = way
	}
	return nil
}
