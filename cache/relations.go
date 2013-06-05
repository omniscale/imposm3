package cache

import (
	"github.com/jmhodges/levigo"
	"goposm/cache/binary"
	"goposm/element"
)

type RelationsCache struct {
	Cache
}

func NewRelationsCache(path string) (*RelationsCache, error) {
	cache := RelationsCache{}
	err := cache.open(path)
	if err != nil {
		return nil, err
	}
	return &cache, err
}

func (p *RelationsCache) PutRelation(relation *element.Relation) error {
	keyBuf := idToKeyBuf(relation.Id)
	data, err := binary.MarshalRelation(relation)
	if err != nil {
		return err
	}
	return p.db.Put(p.wo, keyBuf, data)
}

func (p *RelationsCache) PutRelations(rels []element.Relation) error {
	batch := levigo.NewWriteBatch()
	defer batch.Close()

	for _, rel := range rels {
		if len(rel.Tags) == 0 {
			continue
		}
		keyBuf := idToKeyBuf(rel.Id)
		data, err := binary.MarshalRelation(&rel)
		if err != nil {
			return err
		}
		batch.Put(keyBuf, data)
	}
	return p.db.Write(p.wo, batch)
}

func (p *RelationsCache) Iter() chan *element.Relation {
	rels := make(chan *element.Relation)
	go func() {
		ro := levigo.NewReadOptions()
		ro.SetFillCache(false)
		it := p.db.NewIterator(ro)
		defer it.Close()
		it.SeekToFirst()
		for ; it.Valid(); it.Next() {
			rel, err := binary.UnmarshalRelation(it.Value())
			if err != nil {
				panic(err)
			}
			rel.Id = idFromKeyBuf(it.Key())

			rels <- rel
		}
		close(rels)
	}()
	return rels
}

func (p *RelationsCache) GetRelation(id int64) (*element.Relation, error) {
	keyBuf := idToKeyBuf(id)
	data, err := p.db.Get(p.ro, keyBuf)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, NotFound
	}
	relation, err := binary.UnmarshalRelation(data)
	if err != nil {
		return nil, err
	}
	relation.Id = id
	return relation, err
}
