package cache

import (
	"github.com/jmhodges/levigo"
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

func (p *WaysCache) PutWay(way *osm.Way) error {
	if way.ID == SKIP {
		return nil
	}
	keyBuf := idToKeyBuf(way.ID)
	data, err := binary.MarshalWay(way)
	if err != nil {
		return err
	}
	return p.db.Put(p.wo, keyBuf, data)
}

func (p *WaysCache) PutWays(ways []osm.Way) error {
	batch := levigo.NewWriteBatch()
	defer batch.Close()

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
	return p.db.Write(p.wo, batch)
}

func (p *WaysCache) GetWay(id int64) (*osm.Way, error) {
	keyBuf := idToKeyBuf(id)
	data, err := p.db.Get(p.ro, keyBuf)
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

func (p *WaysCache) DeleteWay(id int64) error {
	keyBuf := idToKeyBuf(id)
	return p.db.Delete(p.wo, keyBuf)
}

func (p *WaysCache) Iter() chan *osm.Way {
	ways := make(chan *osm.Way, 1024)
	go func() {
		ro := levigo.NewReadOptions()
		ro.SetFillCache(false)
		it := p.db.NewIterator(ro)
		// we need to Close the iter before closing the
		// chan (and thus signaling that we are done)
		// to avoid race where db is closed before the iterator
		defer close(ways)
		defer it.Close()
		it.SeekToFirst()
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

func (self *WaysCache) FillMembers(members []osm.Member) error {
	if members == nil || len(members) == 0 {
		return nil
	}
	for i, member := range members {
		if member.Type != osm.WAY {
			continue
		}
		way, err := self.GetWay(member.ID)
		if err != nil {
			return err
		}
		members[i].Way = way
	}
	return nil
}
