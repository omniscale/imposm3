package cache

import (
	"bytes"
	bin "encoding/binary"
	"github.com/jmhodges/levigo"
	"goposm/binary"
	"goposm/element"
	"os"
	"path/filepath"
	"strconv"
)

var levelDbWriteBufferSize, levelDbWriteBlockSize int64
var deltaCacheBunchSize int64

func init() {
	levelDbWriteBufferSize, _ = strconv.ParseInt(
		os.Getenv("GOPOSM_LEVELDB_BUFFERSIZE"), 10, 32)
	levelDbWriteBlockSize, _ = strconv.ParseInt(
		os.Getenv("GOPOSM_LEVELDB_BLOCKSIZE"), 10, 32)

	// bunchSize defines how many coordinates should be stored in a
	// single record. This is the maximum and a bunch will typically contain
	// less coordinates (e.g. when nodes are removes).
	//
	// A higher number improves -read mode (writing the cache) but also
	// increases the overhead during -write mode (reading coords).
	deltaCacheBunchSize, _ = strconv.ParseInt(
		os.Getenv("GOPOSM_DELTACACHE_BUNCHSIZE"), 10, 32)

	if deltaCacheBunchSize == 0 {
		deltaCacheBunchSize = 128
	}
}

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
	opts.SetMaxOpenFiles(64)
	// save a few bytes by allowing leveldb to use delta enconding
	// for up to n keys (instead of only 16)
	opts.SetBlockRestartInterval(128)
	if levelDbWriteBufferSize != 0 {
		opts.SetWriteBufferSize(int(levelDbWriteBufferSize))
	}
	if levelDbWriteBlockSize != 0 {
		opts.SetBlockSize(int(levelDbWriteBlockSize))
	}
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

func idToKeyBuf(id int64) []byte {
	var b bytes.Buffer
	bin.Write(&b, bin.BigEndian, &id)
	return b.Bytes()
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
	keyBuf := idToKeyBuf(node.Id)
	data, err := binary.MarshalCoord(node)
	if err != nil {
		panic(err)
	}
	return p.db.Put(p.wo, keyBuf, data)
}

func (p *CoordsCache) PutCoords(nodes []element.Node) error {
	batch := levigo.NewWriteBatch()
	defer batch.Close()

	for _, node := range nodes {
		keyBuf := idToKeyBuf(node.Id)
		data, err := binary.MarshalCoord(&node)
		if err != nil {
			panic(err)
		}
		batch.Put(keyBuf, data)
	}
	return p.db.Write(p.wo, batch)
}

func (p *CoordsCache) GetCoord(id int64) (*element.Node, error) {
	keyBuf := idToKeyBuf(id)
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
	keyBuf := idToKeyBuf(node.Id)
	data, err := binary.MarshalNode(node)
	if err != nil {
		panic(err)
	}
	return p.db.Put(p.wo, keyBuf, data)
}

func (p *NodesCache) PutNodes(nodes []element.Node) (int, error) {
	batch := levigo.NewWriteBatch()
	defer batch.Close()

	var n int
	for _, node := range nodes {
		if len(node.Tags) == 0 {
			continue
		}
		keyBuf := idToKeyBuf(node.Id)
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
	keyBuf := idToKeyBuf(id)
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
	keyBuf := idToKeyBuf(way.Id)
	data, err := binary.MarshalWay(way)
	if err != nil {
		panic(err)
	}
	return p.db.Put(p.wo, keyBuf, data)
}

func (p *WaysCache) PutWays(ways []element.Way) error {
	batch := levigo.NewWriteBatch()
	defer batch.Close()

	for _, way := range ways {
		keyBuf := idToKeyBuf(way.Id)
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

		for _, way := range ways {
			keyBuf := idToKeyBuf(way.Id)
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
	keyBuf := idToKeyBuf(id)
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
		for ; it.Valid(); it.Next() {
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
	keyBuf := idToKeyBuf(relation.Id)
	data, err := binary.MarshalRelation(relation)
	if err != nil {
		panic(err)
	}
	return p.db.Put(p.wo, keyBuf, data)
}

func (p *RelationsCache) PutRelations(rels []element.Relation) error {
	batch := levigo.NewWriteBatch()
	defer batch.Close()

	for _, rel := range rels {
		keyBuf := idToKeyBuf(rel.Id)
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
		for ; it.Valid(); it.Next() {
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
	keyBuf := idToKeyBuf(id)
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
