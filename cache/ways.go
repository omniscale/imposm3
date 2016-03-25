package cache

import (
	"github.com/jmhodges/levigo"
	"github.com/omniscale/imposm3/cache/binary"
	"github.com/omniscale/imposm3/element"
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

func (p *WaysCache) PutWay(way *element.Way) error {
	if way.Id == SKIP {
		return nil
	}
	keyBuf := idToKeyBuf(way.Id)
	data, err := binary.MarshalWay(way)
	if err != nil {
		return err
	}
	return p.db.Put(p.wo, keyBuf, data)
}

func (p *WaysCache) PutWays(ways []element.Way) error {
	batch := levigo.NewWriteBatch()
	defer batch.Close()

	for _, way := range ways {
		if way.Id == SKIP {
			continue
		}
		keyBuf := idToKeyBuf(way.Id)
		data, err := binary.MarshalWay(&way)
		if err != nil {
			return err
		}
		batch.Put(keyBuf, data)
	}
	return p.db.Write(p.wo, batch)
}

func (p *WaysCache) GetWay(id int64) (*element.Way, error) {
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
	way.Id = id
	return way, nil
}

func (p *WaysCache) DeleteWay(id int64) error {
	keyBuf := idToKeyBuf(id)
	return p.db.Delete(p.wo, keyBuf)
}

func (p *WaysCache) Iter() chan *element.Way {
	ways := make(chan *element.Way, 1024)
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
			way.Id = idFromKeyBuf(it.Key())
			ways <- way
		}
	}()
	return ways
}

func (self *WaysCache) FillMembers(members []element.Member) error {
	if members == nil || len(members) == 0 {
		return nil
	}
	for i, member := range members {
		if member.Type != element.WAY {
			continue
		}
		way, err := self.GetWay(member.Id)
		if err != nil {
			return err
		}
		members[i].Way = way
	}
	return nil
}

type InsertedWaysCache struct {
	cache
}

func newInsertedWaysCache(path string) (*InsertedWaysCache, error) {
	cache := InsertedWaysCache{}
	cache.options = &globalCacheOptions.InsertedWays

	err := cache.open(path)
	if err != nil {
		return nil, err
	}
	return &cache, err
}

func (p *InsertedWaysCache) PutWay(way *element.Way) error {
	keyBuf := idToKeyBuf(way.Id)
	return p.db.Put(p.wo, keyBuf, []byte{})
}

func (p *InsertedWaysCache) PutMembers(members []element.Member) error {
	batch := levigo.NewWriteBatch()
	defer batch.Close()

	for _, m := range members {
		if m.Type != element.WAY {
			continue
		}
		keyBuf := idToKeyBuf(m.Id)
		batch.Put(keyBuf, []byte{})
	}
	return p.db.Write(p.wo, batch)
}

func (p *InsertedWaysCache) DeleteMembers(members []element.Member) error {
	batch := levigo.NewWriteBatch()
	defer batch.Close()

	for _, m := range members {
		if m.Type != element.WAY {
			continue
		}
		keyBuf := idToKeyBuf(m.Id)
		batch.Delete(keyBuf)
	}
	return p.db.Write(p.wo, batch)
}

func (p *InsertedWaysCache) IsInserted(id int64) (bool, error) {
	keyBuf := idToKeyBuf(id)
	data, err := p.db.Get(p.ro, keyBuf)
	if err != nil {
		return false, err
	}
	if data == nil {
		return false, nil
	}
	return true, nil
}
