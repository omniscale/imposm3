package cache

import (
	"github.com/dgraph-io/badger"
	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/cache/binary"
)

type RelationsCache struct {
	cache
}

func newRelationsCache(path string) (*RelationsCache, error) {
	cache := RelationsCache{}
	cache.options = &globalCacheOptions.Relations
	err := cache.open(path)
	if err != nil {
		return nil, err
	}
	return &cache, err
}

func (p *RelationsCache) PutRelation(relation *osm.Relation) error {
	if relation.ID == SKIP {
		return nil
	}
	keyBuf := idToKeyBuf(relation.ID)
	data, err := binary.MarshalRelation(relation)
	if err != nil {
		return err
	}
	return p.db.Put(keyBuf, data)
}

func (p *RelationsCache) PutRelations(rels []osm.Relation) error {
	batch := p.db.NewWriteBatch()
	defer batch.Cancel()

	for _, rel := range rels {
		if rel.ID == SKIP {
			continue
		}
		if len(rel.Tags) == 0 {
			continue
		}
		keyBuf := idToKeyBuf(rel.ID)
		data, err := binary.MarshalRelation(&rel)
		if err != nil {
			return err
		}
		batch.Set(keyBuf, data, 0)
	}
	return batch.Flush()
}

func (p *RelationsCache) Iter() chan *osm.Relation {
	rels := make(chan *osm.Relation)
	go func() {
		ro := badger.DefaultIteratorOptions
		ro.PrefetchSize = 100
		_ = p.db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(ro)
			// we need to Close the iter before closing the
			// chan (and thus signaling that we are done)
			// to avoid race where db is closed before the iterator
			defer close(rels)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				var rel *osm.Relation
				_ = it.Item().Value(func(val []byte) error {
					var err error
					rel, err = binary.UnmarshalRelation(val)
					if err != nil {
						panic(err)
					}
					return err
				})

				rel.ID = idFromKeyBuf(it.Item().Key())

				rels <- rel
			}
			return  nil
		})
	}()
	return rels
}

func (p *RelationsCache) GetRelation(id int64) (*osm.Relation, error) {
	keyBuf := idToKeyBuf(id)
	data, err := p.db.Get(keyBuf)
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
	relation.ID = id
	return relation, err
}

func (p *RelationsCache) DeleteRelation(id int64) error {
	keyBuf := idToKeyBuf(id)
	return p.db.Delete(keyBuf)
}
