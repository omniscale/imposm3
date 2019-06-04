package cache

import (
	"github.com/dgraph-io/badger"
	"github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/cache/binary"
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
	return p.db.Put(keyBuf, data)
}

func (p *NodesCache) PutNodes(nodes []osm.Node) (int, error) {
	batch := p.db.NewWriteBatch()
	defer batch.Cancel()

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
		batch.Set(keyBuf, data, 0)
		n++
	}
	return n, batch.Flush()
}

func (p *NodesCache) GetNode(id int64) (*osm.Node, error) {
	keyBuf := idToKeyBuf(id)
	data, err := p.db.Get(keyBuf)
	if err != nil {
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
	return p.db.Delete(keyBuf)
}

func (p *NodesCache) Iter() chan *osm.Node {
	nodes := make(chan *osm.Node)
	go func() {
		ro := badger.DefaultIteratorOptions
		ro.PrefetchSize = 100

		_ = p.db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(ro)
			// we need to Close the iter before closing the
			// chan (and thus signaling that we are done)
			// to avoid race where db is closed before the iterator
			defer close(nodes)
			defer it.Close()
			var node *osm.Node
			for it.Rewind(); it.Valid(); it.Next() {
				_ = it.Item().Value(func(val []byte) error {
					var err error
					node, err = binary.UnmarshalNode(val)
					if err != nil {
						panic(err)
					}
					return err
				})
				node.ID = idFromKeyBuf(it.Item().Key())

				nodes <- node
			}
			return nil
		})
	}()
	return nodes
}
