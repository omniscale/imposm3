package cache

import (
	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/cache/binary"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type NodesCache struct {
	cache
}

func newNodesCache(path string) (*NodesCache, error) {
	cache := NodesCache{}
	cache.options = &globalCacheOptions.Nodes
	err := cache.open(path)
	if err != nil {
		return nil, err
	}
	return &cache, err
}

func (p *NodesCache) PutNode(node *osm.Node) error {
	if node.ID == SKIP {
		return nil
	}
	if node.Tags == nil {
		return nil
	}
	keyBuf := idToKeyBuf(node.ID)
	data, err := binary.MarshalNode(node)
	if err != nil {
		return err
	}
	return p.db.Put(keyBuf, data, p.wo)
}

func (p *NodesCache) PutNodes(nodes []osm.Node) (int, error) {
	batch := new(leveldb.Batch)

	var n int
	for _, node := range nodes {
		if node.ID == SKIP {
			continue
		}
		if len(node.Tags) == 0 {
			continue
		}
		keyBuf := idToKeyBuf(node.ID)
		data, err := binary.MarshalNode(&node)
		if err != nil {
			return 0, err
		}
		batch.Put(keyBuf, data)
		n++
	}
	return n, p.db.Write(batch, p.wo)
}

func (p *NodesCache) GetNode(id int64) (*osm.Node, error) {
	keyBuf := idToKeyBuf(id)
	data, err := p.db.Get(keyBuf, p.ro)
	if err != nil && err != leveldb.ErrNotFound {
		return nil, err
	}
	if data == nil {
		return nil, NotFound
	}
	node, err := binary.UnmarshalNode(data)
	if err != nil {
		return nil, err
	}
	node.ID = id
	return node, nil
}

func (p *NodesCache) DeleteNode(id int64) error {
	keyBuf := idToKeyBuf(id)
	return p.db.Delete(keyBuf, p.wo)
}

func (p *NodesCache) Iter() chan *osm.Node {
	nodes := make(chan *osm.Node)
	go func() {
		ro := opt.ReadOptions{}
		ro.DontFillCache = true
		it := p.db.NewIterator(nil, &ro)
		// we need to Close the iter before closing the
		// chan (and thus signaling that we are done)
		// to avoid race where db is closed before the iterator
		defer close(nodes)
		defer it.Release()
		it.First()
		for ; it.Valid(); it.Next() {
			node, err := binary.UnmarshalNode(it.Value())
			if err != nil {
				panic(err)
			}
			node.ID = idFromKeyBuf(it.Key())

			nodes <- node
		}
	}()
	return nodes
}
