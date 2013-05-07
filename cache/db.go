package cache

import (
	bin "encoding/binary"
	"github.com/jmhodges/levigo"
	"goposm/binary"
	"goposm/element"
	"os"
	"path/filepath"
)

type OSMCache struct {
	Dir       string
	Coords    *DeltaCoordsCache
	Ways      *WaysCache
	Nodes     *NodesCache
	Relations *RelationsCache
	opened    bool
}

func (c *OSMCache) Close() {
	if c.Coords != nil {
		c.Coords.close()
		c.Coords = nil
	}
	if c.Nodes != nil {
		c.Nodes.close()
		c.Nodes = nil
	}
	if c.Ways != nil {
		c.Ways.close()
		c.Ways = nil
	}
	if c.Relations != nil {
		c.Relations.close()
		c.Relations = nil
	}
}

func NewOSMCache(dir string) *OSMCache {
	cache := &OSMCache{Dir: dir}
	return cache
}

func (c *OSMCache) Open() error {
	var err error
	c.Coords, err = NewDeltaCoordsCache(filepath.Join(c.Dir, "coords"))
	if err != nil {
		return err
	}
	c.Nodes, err = NewNodesCache(filepath.Join(c.Dir, "nodes"))
	if err != nil {
		c.Close()
		return err
	}
	c.Ways, err = NewWaysCache(filepath.Join(c.Dir, "ways"))
	if err != nil {
		c.Close()
		return err
	}
	c.Relations, err = NewRelationsCache(filepath.Join(c.Dir, "relations"))
	if err != nil {
		c.Close()
		return err
	}
	c.opened = true
	return nil
}

func (c *OSMCache) Exists() bool {
	if c.opened {
		return true
	}
	if _, err := os.Stat(filepath.Join(c.Dir, "coords")); !os.IsNotExist(err) {
		return true
	}
	if _, err := os.Stat(filepath.Join(c.Dir, "nodes")); !os.IsNotExist(err) {
		return true
	}
	if _, err := os.Stat(filepath.Join(c.Dir, "ways")); !os.IsNotExist(err) {
		return true
	}
	if _, err := os.Stat(filepath.Join(c.Dir, "relations")); !os.IsNotExist(err) {
		return true
	}
	return false
}

func (c *OSMCache) Remove() error {
	if c.opened {
		c.Close()
	}
	if err := os.RemoveAll(filepath.Join(c.Dir, "coords")); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(c.Dir, "nodes")); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(c.Dir, "ways")); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(c.Dir, "relations")); err != nil {
		return err
	}
	return nil
}

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

func (c *Cache) close() {
	c.db.Close()
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
	if node.Tags == nil {
		return nil
	}
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, int64(node.Id))
	data, err := binary.MarshalNode(node)
	if err != nil {
		panic(err)
	}
	return p.db.Put(p.wo, keyBuf, data)
}

func (p *NodesCache) PutNodes(nodes []element.Node) (int, error) {
	batch := levigo.NewWriteBatch()
	defer batch.Close()

	keyBuf := make([]byte, 8)
	var n int
	for _, node := range nodes {
		if len(node.Tags) == 0 {
			continue
		}
		bin.PutVarint(keyBuf, int64(node.Id))
		data, err := binary.MarshalNode(&node)
		if err != nil {
			panic(err)
		}
		batch.Put(keyBuf, data)
		n += 1
	}
	return n, p.db.Write(p.wo, batch)
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

func (p *WaysCache) Iter() chan *element.Way {
	way := make(chan *element.Way)
	go func() {
		ro := levigo.NewReadOptions()
		ro.SetFillCache(false)
		it := p.db.NewIterator(ro)
		defer it.Close()
		it.SeekToFirst()
		for it = it; it.Valid(); it.Next() {
			ways, err := binary.UnmarshalWay(it.Value())
			if err != nil {
				panic(err)
			}
			way <- ways
		}
		close(way)
	}()
	return way
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

func (p *RelationsCache) Iter() chan *element.Relation {
	rel := make(chan *element.Relation)
	go func() {
		ro := levigo.NewReadOptions()
		ro.SetFillCache(false)
		it := p.db.NewIterator(ro)
		defer it.Close()
		it.SeekToFirst()
		for it = it; it.Valid(); it.Next() {
			relation, err := binary.UnmarshalRelation(it.Value())
			if err != nil {
				panic(err)
			}
			rel <- relation
		}
		close(rel)
	}()
	return rel
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
