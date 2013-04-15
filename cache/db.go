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

func NewCache(path string) Cache {
	result := Cache{}
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(1024 * 1024 * 50))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open(path, opts)
	if err != nil {
		panic("unable to open db")
	}
	result.db = db
	result.wo = levigo.NewWriteOptions()
	result.ro = levigo.NewReadOptions()
	return result
}

type NodesCache struct {
	Cache
}

func NewNodesCache(path string) *NodesCache {
	cache := NewCache(path)
	nodesCache := NodesCache{cache}
	return &nodesCache
}

type CoordsCache struct {
	Cache
}

func NewCoordsCache(path string) *CoordsCache {
	cache := NewCache(path)
	coordsCache := CoordsCache{cache}
	return &coordsCache
}

type WaysCache struct {
	Cache
}

func NewWaysCache(path string) *WaysCache {
	cache := NewCache(path)
	waysCache := WaysCache{cache}
	return &waysCache
}

type RelationsCache struct {
	Cache
}

func NewRelationsCache(path string) *RelationsCache {
	cache := NewCache(path)
	relationsCache := RelationsCache{cache}
	return &relationsCache
}

func (p *CoordsCache) PutCoord(node *element.Node) {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(node.Id))
	data, err := binary.MarshalCoord(node)
	if err != nil {
		panic(err)
	}
	p.db.Put(p.wo, keyBuf, data)
}

func (p *CoordsCache) PutCoords(nodes []element.Node) {
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
	p.db.Write(p.wo, batch)
}

func (p *CoordsCache) GetCoord(id int64) *element.Node {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(id))
	data, err := p.db.Get(p.ro, keyBuf)
	if err != nil {
		panic(err)
	}
	if data == nil {
		return nil
	}

	node, err := binary.UnmarshalCoord(id, data)
	if err != nil {
		panic(err)
	}
	return node
}

func (p *NodesCache) PutNode(node *element.Node) {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(node.Id))
	data, err := binary.MarshalNode(node)
	if err != nil {
		panic(err)
	}
	p.db.Put(p.wo, keyBuf, data)
}

func (p *NodesCache) GetNode(id int64) *element.Node {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(id))
	data, err := p.db.Get(p.ro, keyBuf)
	if err != nil {
		panic(err)
	}
	if data == nil {
		return nil
	}
	node, err := binary.UnmarshalNode(data)
	if err != nil {
		panic(err)
	}
	return node
}

func (p *WaysCache) PutWay(way *element.Way) {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(way.Id))
	data, err := binary.MarshalWay(way)
	if err != nil {
		panic(err)
	}
	p.db.Put(p.wo, keyBuf, data)
}

func (p *WaysCache) PutWays(ways []element.Way) {
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
	p.db.Write(p.wo, batch)
}

func (p *WaysCache) GetWay(id int64) *element.Way {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(id))
	data, err := p.db.Get(p.ro, keyBuf)
	if err != nil {
		panic(err)
	}
	if data == nil {
		return nil
	}
	way, err := binary.UnmarshalWay(data)
	if err != nil {
		panic(err)
	}
	return way
}

func (p *RelationsCache) PutRelation(relation *element.Relation) {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(relation.Id))
	data, err := binary.MarshalRelation(relation)
	if err != nil {
		panic(err)
	}
	p.db.Put(p.wo, keyBuf, data)
}

func (p *RelationsCache) GetRelation(id int64) *element.Relation {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(id))
	data, err := p.db.Get(p.ro, keyBuf)
	if err != nil {
		panic(err)
	}
	if data == nil {
		return nil
	}
	relation, err := binary.UnmarshalRelation(data)
	if err != nil {
		panic(err)
	}
	return relation
}

func (p *Cache) Close() {
	p.db.Close()
}
