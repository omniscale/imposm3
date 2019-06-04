package cache

import (
	"github.com/dgraph-io/badger"
	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/cache/binary"
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
	return c.db.Put(keyBuf, data)
}

func (c *WaysCache) PutWays(ways []osm.Way) error {
	batch := c.db.NewWriteBatch()
	defer batch.Cancel()

	for _, way := range ways {
		if way.ID == SKIP {
			continue
		}
		keyBuf := idToKeyBuf(way.ID)
		data, err := binary.MarshalWay(&way)
		if err != nil {
			return err
		}
		batch.Set(keyBuf, data, 0)
	}
	return batch.Flush()
}

func (c *WaysCache) GetWay(id int64) (*osm.Way, error) {
	keyBuf := idToKeyBuf(id)
	data, err := c.db.Get(keyBuf)
	if err != nil {
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
	return c.db.Delete(keyBuf)
}

func (c *WaysCache) Iter() chan *osm.Way {
	ways := make(chan *osm.Way, 1024)
	go func() {
		ro := badger.DefaultIteratorOptions
		ro.PrefetchSize = 100
		_ = c.db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(ro)
			// we need to Close the iter before closing the
			// chan (and thus signaling that we are done)
			// to avoid race where db is closed before the iterator
			defer close(ways)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				var way *osm.Way
				_ = it.Item().Value(func(val []byte) error {
					var err error
					way, err = binary.UnmarshalWay(val)
					if err != nil {
						panic(err)
					}
					return err
				})
				way.ID = idFromKeyBuf(it.Item().Key())
				ways <- way
			}
			return nil
		})

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
