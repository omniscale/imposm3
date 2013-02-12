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

func NewCache(path string) *Cache {
	result := &Cache{}
	opts := levigo.NewOptions()
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
	data, err := binary.MarshalNode(node)
	if err != nil {
		panic(err)
	}
	p.db.Put(p.wo, keyBuf, data)
}

func (p *Cache) GetCoord(id element.OSMID) *element.Node {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(id))
	data, err := p.db.Get(p.ro, keyBuf)
	if err != nil {
		panic(err)
	}
	node, err := binary.UnmarshalNode(data)
	if err != nil {
		panic(err)
	}
	return node
}

func (p *Cache) Close() {
	p.db.Close()
}
