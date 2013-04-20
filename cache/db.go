package cache

import (
	bin "encoding/binary"
	"github.com/jmhodges/levigo"
	"goposm/binary"
	"goposm/element"
)

type Cache struct {
	db *levigo.DB
	wo *levigo.WriteOptions
	ro *levigo.ReadOptions
}

func (c *Cache) open(path string) error {
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(1024 * 1024 * 50))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open(path, opts)
	if err != nil {
		return err
	}
	c.db = db
	c.wo = levigo.NewWriteOptions()
	c.ro = levigo.NewReadOptions()
	return nil
}

type NodesCache struct {
	Cache
}

func NewNodesCache(path string) (*NodesCache, error) {
	cache := NodesCache{}
	err := cache.open(path)
	if err != nil {
		return nil, err
	}
	return &cache, err
}

type CoordsCache struct {
	Cache
}

func NewCoordsCache(path string) (*CoordsCache, error) {
	cache := CoordsCache{}
	err := cache.open(path)
	if err != nil {
		return nil, err
	}
	return &cache, err
}

type WaysCache struct {
	Cache
	toWrite chan []element.Way
}

func NewWaysCache(path string) (*WaysCache, error) {
	cache := WaysCache{}
	cache.toWrite = make(chan []element.Way)
	go cache.wayWriter()
	err := cache.open(path)
	if err != nil {
		return nil, err
	}
	return &cache, err
}

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

func (p *CoordsCache) PutCoord(node *element.Node) error {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(node.Id))
	data, err := binary.MarshalCoord(node)
	if err != nil {
		panic(err)
	}
	return p.db.Put(p.wo, keyBuf, data)
}

func (p *CoordsCache) PutCoords(nodes []element.Node) error {
	batch := levigo.NewWriteBatch()
	defer batch.Close()

	keyBuf := make([]byte, 8)
	for _, node := range nodes {
		bin.PutVarint(keyBuf, int64(node.Id))
		data, err := binary.MarshalCoord(&node)
		if err != nil {
			panic(err)
		}
		batch.Put(keyBuf, data)
	}
	return p.db.Write(p.wo, batch)
}

func (p *CoordsCache) GetCoord(id int64) (*element.Node, error) {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(id))
	data, err := p.db.Get(p.ro, keyBuf)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}

	node, err := binary.UnmarshalCoord(id, data)
	if err != nil {
		panic(err)
	}
	return node, nil
}

func (p *NodesCache) PutNode(node *element.Node) error {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(node.Id))
	data, err := binary.MarshalNode(node)
	if err != nil {
		panic(err)
	}
	return p.db.Put(p.wo, keyBuf, data)
}

func (p *NodesCache) GetNode(id int64) (*element.Node, error) {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(id))
	data, err := p.db.Get(p.ro, keyBuf)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	node, err := binary.UnmarshalNode(data)
	if err != nil {
		panic(err)
	}
	return node, nil
}

func (p *WaysCache) PutWay(way *element.Way) error {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(way.Id))
	data, err := binary.MarshalWay(way)
	if err != nil {
		panic(err)
	}
	return p.db.Put(p.wo, keyBuf, data)
}

func (p *WaysCache) PutWays(ways []element.Way) error {
	batch := levigo.NewWriteBatch()
	defer batch.Close()

	keyBuf := make([]byte, 8)
	for _, way := range ways {
		bin.PutVarint(keyBuf, int64(way.Id))
		data, err := binary.MarshalWay(&way)
		if err != nil {
			panic(err)
		}
		batch.Put(keyBuf, data)
	}
	return p.db.Write(p.wo, batch)
}

func (p *WaysCache) _PutWays(ways []element.Way) {
	p.toWrite <- ways
}

func (p *WaysCache) wayWriter() {
	for ways := range p.toWrite {
		batch := levigo.NewWriteBatch()
		defer batch.Close()

		keyBuf := make([]byte, 8)
		for _, way := range ways {
			bin.PutVarint(keyBuf, int64(way.Id))
			data, err := binary.MarshalWay(&way)
			if err != nil {
				panic(err)
			}
			batch.Put(keyBuf, data)
		}
		_ = p.db.Write(p.wo, batch)
	}
}

func (p *WaysCache) GetWay(id int64) (*element.Way, error) {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(id))
	data, err := p.db.Get(p.ro, keyBuf)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	way, err := binary.UnmarshalWay(data)
	if err != nil {
		panic(err)
	}
	return way, nil
}

func (p *RelationsCache) PutRelation(relation *element.Relation) error {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(relation.Id))
	data, err := binary.MarshalRelation(relation)
	if err != nil {
		panic(err)
	}
	return p.db.Put(p.wo, keyBuf, data)
}

func (p *RelationsCache) PutRelations(rels []element.Relation) error {
	batch := levigo.NewWriteBatch()
	defer batch.Close()

	keyBuf := make([]byte, 8)
	for _, rel := range rels {
		bin.PutVarint(keyBuf, int64(rel.Id))
		data, err := binary.MarshalRelation(&rel)
		if err != nil {
			panic(err)
		}
		batch.Put(keyBuf, data)
	}
	return p.db.Write(p.wo, batch)
}

func (p *RelationsCache) GetRelation(id int64) (*element.Relation, error) {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(id))
	data, err := p.db.Get(p.ro, keyBuf)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	relation, err := binary.UnmarshalRelation(data)
	if err != nil {
		panic(err)
	}
	return relation, err
}

func (p *Cache) Close() {
	p.db.Close()
}
