package cache

import (
	"code.google.com/p/goprotobuf/proto"
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

func NewCache(path string) *Cache {
	result := &Cache{}
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

func (p *Cache) PutCoord(node *element.Node) {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(node.Id))
	data, err := binary.MarshalCoord(node)
	if err != nil {
		panic(err)
	}
	p.db.Put(p.wo, keyBuf, data)
}

func (p *Cache) PutCoords(nodes []element.Node) {
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

func (p *Cache) PutCoordsPacked(nodes []element.Node) {
	if len(nodes) == 0 {
		return
	}
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(nodes[0].Id))

	deltaCoords := packNodes(nodes)
	data, err := proto.Marshal(deltaCoords)
	if err != nil {
		panic(err)
	}
	p.db.Put(p.wo, keyBuf, data)
}

func (p *Cache) GetCoord(id int64) *element.Node {
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

func (p *Cache) PutNode(node *element.Node) {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(node.Id))
	data, err := binary.MarshalNode(node)
	if err != nil {
		panic(err)
	}
	p.db.Put(p.wo, keyBuf, data)
}

func (p *Cache) GetNode(id int64) *element.Node {
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

func (p *Cache) PutWay(way *element.Way) {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(way.Id))
	data, err := binary.MarshalWay(way)
	if err != nil {
		panic(err)
	}
	p.db.Put(p.wo, keyBuf, data)
}

func (p *Cache) PutWays(ways []element.Way) {
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

func (p *Cache) GetWay(id int64) *element.Way {
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

func (p *Cache) PutRelation(relation *element.Relation) {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(relation.Id))
	data, err := binary.MarshalRelation(relation)
	if err != nil {
		panic(err)
	}
	p.db.Put(p.wo, keyBuf, data)
}

func (p *Cache) GetRelation(id int64) *element.Relation {
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
