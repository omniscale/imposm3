package cache

import (
	"container/list"
	"sort"
	"sync"

	"github.com/omniscale/imposm3/cache/binary"
	"github.com/omniscale/imposm3/element"
)

type byId []element.Node

func (s byId) Len() int           { return len(s) }
func (s byId) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s byId) Less(i, j int) bool { return s[i].Id < s[j].Id }

type coordsBunch struct {
	sync.Mutex
	id         int64
	coords     []element.Node
	elem       *list.Element
	needsWrite bool
}

func (b *coordsBunch) GetCoord(id int64) (*element.Node, error) {
	idx := sort.Search(len(b.coords), func(i int) bool {
		return b.coords[i].Id >= id
	})
	if idx < len(b.coords) && b.coords[idx].Id == id {
		nd := b.coords[idx] // create copy prevent to race when node gets reprojected
		return &nd, nil
	}
	return nil, NotFound
}

func (b *coordsBunch) DeleteCoord(id int64) {
	idx := sort.Search(len(b.coords), func(i int) bool {
		return b.coords[i].Id >= id
	})
	if idx < len(b.coords) && b.coords[idx].Id == id {
		b.coords = append(b.coords[:idx], b.coords[idx+1:]...)
	}
}

// PutCoord puts a single coord into the coords bunch. This function
// does support updating nodes.
func (b *coordsBunch) PutCoord(node element.Node) {
	idx := sort.Search(len(b.coords), func(i int) bool {
		return b.coords[i].Id >= node.Id
	})
	if idx < len(b.coords) {
		if b.coords[idx].Id == node.Id {
			// overwrite
			b.coords[idx] = node
		} else {
			// insert
			b.coords = append(b.coords, node)
			copy(b.coords[idx+1:], b.coords[idx:])
			b.coords[idx] = node
		}
	} else {
		// append
		b.coords = append(b.coords, node)
	}
}

// PutCoords puts multiple coords into the coords bunch. This bulk function
// does not support duplicate or updated nodes.
func (b *coordsBunch) PutCoords(nodes []element.Node) {
	b.coords = append(b.coords, nodes...)
	sort.Sort(byId(b.coords))
}

type DeltaCoordsCache struct {
	cache
	lruList      *list.List
	table        map[int64]*coordsBunch
	capacity     int64
	linearImport bool
	mu           sync.Mutex
	bunchSize    int64
	readOnly     bool
}

func newDeltaCoordsCache(path string) (*DeltaCoordsCache, error) {
	coordsCache := DeltaCoordsCache{}
	coordsCache.options = &globalCacheOptions.Coords.cacheOptions
	err := coordsCache.open(path)
	if err != nil {
		return nil, err
	}
	coordsCache.bunchSize = int64(globalCacheOptions.Coords.BunchSize)
	coordsCache.lruList = list.New()
	// mem req for cache approx. capacity*bunchSize*40
	coordsCache.capacity = int64(globalCacheOptions.Coords.BunchCacheCapacity)
	coordsCache.table = make(map[int64]*coordsBunch, coordsCache.capacity)
	return &coordsCache, nil
}

func (self *DeltaCoordsCache) SetLinearImport(v bool) {
	self.linearImport = v
}

func (self *DeltaCoordsCache) Flush() error {
	self.mu.Lock()
	defer self.mu.Unlock()
	for bunchId, bunch := range self.table {
		if bunch.needsWrite {
			err := self.putCoordsPacked(bunchId, bunch.coords)
			if err != nil {
				return err
			}
		}
	}

	self.lruList.Init()
	for k, _ := range self.table {
		delete(self.table, k)
	}
	return nil
}
func (self *DeltaCoordsCache) Close() error {
	err := self.Flush()
	if err != nil {
		return err
	}
	self.cache.Close()
	return nil
}

func (self *DeltaCoordsCache) SetReadOnly(val bool) {
	self.readOnly = val
}

func (self *DeltaCoordsCache) GetCoord(id int64) (*element.Node, error) {
	bunchId := self.getBunchId(id)
	bunch, err := self.getBunch(bunchId)
	if err != nil {
		return nil, err
	}
	if self.readOnly {
		bunch.Unlock()
	} else {
		defer bunch.Unlock()
	}
	return bunch.GetCoord(id)
}

func (self *DeltaCoordsCache) DeleteCoord(id int64) error {
	bunchId := self.getBunchId(id)
	bunch, err := self.getBunch(bunchId)
	if err != nil {
		return err
	}
	defer bunch.Unlock()
	bunch.DeleteCoord(id)
	bunch.needsWrite = true
	return nil
}

func (self *DeltaCoordsCache) FillWay(way *element.Way) error {
	if way == nil {
		return nil
	}
	way.Nodes = make([]element.Node, len(way.Refs))

	var err error
	var bunch *coordsBunch
	var bunchId, lastBunchId int64
	lastBunchId = -1

	for i, id := range way.Refs {
		bunchId = self.getBunchId(id)
		// re-use bunches
		if bunchId != lastBunchId {
			if bunch != nil {
				bunch.Unlock()
			}
			bunch, err = self.getBunch(bunchId)
			if err != nil {
				return err
			}
		}
		lastBunchId = bunchId

		nd, err := bunch.GetCoord(id)
		if err != nil {
			bunch.Unlock()
			return err
		}
		way.Nodes[i] = *nd
	}
	if bunch != nil {
		bunch.Unlock()
	}
	return nil
}

func removeSkippedNodes(nodes []element.Node) []element.Node {
	insertPoint := 0
	for i := 0; i < len(nodes); i++ {
		if i != insertPoint {
			nodes[insertPoint] = nodes[i]
		}
		if nodes[i].Id != SKIP {
			insertPoint += 1
		}
	}
	return nodes[:insertPoint]
}

// PutCoords puts nodes into cache.
// nodes need to be sorted by Id.
func (self *DeltaCoordsCache) PutCoords(nodes []element.Node) error {
	var start, currentBunchId int64
	nodes = removeSkippedNodes(nodes)
	if len(nodes) == 0 {
		// skipped all nodes
		return nil
	}
	currentBunchId = self.getBunchId(nodes[0].Id)
	start = 0
	totalNodes := len(nodes)
	for i, node := range nodes {
		bunchId := self.getBunchId(node.Id)
		if bunchId != currentBunchId {
			if self.linearImport && int64(i) > self.bunchSize && int64(i) < int64(totalNodes)-self.bunchSize {
				// no need to handle concurrent updates to the same
				// bunch if we are not at the boundary of a self.bunchSize
				err := self.putCoordsPacked(currentBunchId, nodes[start:i])
				if err != nil {
					return err
				}
			} else {
				bunch, err := self.getBunch(currentBunchId)
				if err != nil {
					return err
				}
				if self.linearImport {
					bunch.PutCoords(nodes[start:i])
				} else {
					for _, node := range nodes[start:i] {
						// single inserts to handle updated coords
						bunch.PutCoord(node)
					}
				}
				bunch.needsWrite = true
				bunch.Unlock()
			}
			currentBunchId = bunchId
			start = int64(i)
		}
	}
	bunch, err := self.getBunch(currentBunchId)
	if err != nil {
		return err
	}

	if self.linearImport {
		bunch.PutCoords(nodes[start:])
	} else {
		for _, node := range nodes[start:] {
			// single inserts to handle updated coords
			bunch.PutCoord(node)
		}
	}

	bunch.needsWrite = true
	bunch.Unlock()
	return nil
}

func (p *DeltaCoordsCache) putCoordsPacked(bunchId int64, nodes []element.Node) error {
	keyBuf := idToKeyBuf(bunchId)

	if len(nodes) == 0 {
		return p.db.Delete(p.wo, keyBuf)
	}

	data := make([]byte, 512)
	data = binary.MarshalDeltaNodes(nodes, data)

	err := p.db.Put(p.wo, keyBuf, data)
	if err != nil {
		return err
	}

	return nil
}

func (p *DeltaCoordsCache) getCoordsPacked(bunchId int64, nodes []element.Node) ([]element.Node, error) {
	keyBuf := idToKeyBuf(bunchId)

	data, err := p.db.Get(p.ro, keyBuf)
	if err != nil {
		return nil, err
	}
	if data == nil {
		// clear before returning
		return nodes[:0], nil
	}
	nodes, err = binary.UnmarshalDeltaNodes(data, nodes)
	if err != nil {
		return nil, err
	}

	return nodes, nil
}

func (self *DeltaCoordsCache) getBunchId(nodeId int64) int64 {
	return nodeId / self.bunchSize
}

func (self *DeltaCoordsCache) getBunch(bunchId int64) (*coordsBunch, error) {
	self.mu.Lock()
	bunch, ok := self.table[bunchId]
	var nodes []element.Node
	needsGet := false
	if !ok {
		elem := self.lruList.PushFront(bunchId)
		nodes = make([]element.Node, 0, self.bunchSize)
		bunch = &coordsBunch{id: bunchId, coords: nodes, elem: elem}
		needsGet = true
		self.table[bunchId] = bunch
	} else {
		self.lruList.MoveToFront(bunch.elem)
	}
	bunch.Lock()
	err := self.CheckCapacity()
	self.mu.Unlock()
	if err != nil {
		return nil, err
	}

	if needsGet {
		nodes, err := self.getCoordsPacked(bunchId, nodes)
		if err != nil {
			return nil, err
		}
		bunch.coords = nodes
	}

	return bunch, nil
}

func (self *DeltaCoordsCache) CheckCapacity() error {
	for int64(len(self.table)) > self.capacity {
		elem := self.lruList.Back()
		bunchId := self.lruList.Remove(elem).(int64)
		bunch := self.table[bunchId]
		bunch.elem = nil
		if bunch.needsWrite {
			if err := self.putCoordsPacked(bunchId, bunch.coords); err != nil {
				return err
			}
		}
		delete(self.table, bunchId)
	}
	return nil
}

func (self *DeltaCoordsCache) FirstRefIsCached(refs []int64) (bool, error) {
	if len(refs) <= 0 {
		return false, nil
	}
	_, err := self.GetCoord(refs[0])
	if err == NotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
