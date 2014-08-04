package cache

import (
	"github.com/jmhodges/levigo"
	"github.com/omniscale/imposm3/cache/binary"
	"github.com/omniscale/imposm3/element"
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

func (p *NodesCache) PutNode(node *element.Node) error {
	if node.Id == SKIP {
		return nil
	}
	if node.Tags == nil {
		return nil
	}
	keyBuf := idToKeyBuf(node.Id)
	data, err := binary.MarshalNode(node)
	if err != nil {
		return err
	}
	return p.db.Put(p.wo, keyBuf, data)
}

func (p *NodesCache) PutNodes(nodes []element.Node) (int, error) {
	batch := levigo.NewWriteBatch()
	defer batch.Close()

	var n int
	for _, node := range nodes {
		if node.Id == SKIP {
			continue
		}
		if len(node.Tags) == 0 {
			continue
		}
		keyBuf := idToKeyBuf(node.Id)
		data, err := binary.MarshalNode(&node)
		if err != nil {
			return 0, err
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
		return nil, NotFound
	}
	node, err := binary.UnmarshalNode(data)
	if err != nil {
		return nil, err
	}
	node.Id = id
	return node, nil
}

func (p *NodesCache) DeleteNode(id int64) error {
	keyBuf := idToKeyBuf(id)
	return p.db.Delete(p.wo, keyBuf)
}

func (p *NodesCache) Iter() chan *element.Node {
	nodes := make(chan *element.Node)
	go func() {
		ro := levigo.NewReadOptions()
		ro.SetFillCache(false)
		it := p.db.NewIterator(ro)
		// we need to Close the iter before closing the
		// chan (and thus signaling that we are done)
		// to avoid race where db is closed before the iterator
		defer close(nodes)
		defer it.Close()
		it.SeekToFirst()
		for ; it.Valid(); it.Next() {
			node, err := binary.UnmarshalNode(it.Value())
			if err != nil {
				panic(err)
			}
			node.Id = idFromKeyBuf(it.Key())

			nodes <- node
		}
	}()
	return nodes
}
