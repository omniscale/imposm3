package cache

import (
	"container/list"
	"sort"
	"sync"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/cache/binary"
	"github.com/syndtr/goleveldb/leveldb"
)

type byID []osm.Node

func (s byID) Len() int           { return len(s) }
func (s byID) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s byID) Less(i, j int) bool { return s[i].ID < s[j].ID }

type coordsBunch struct {
	sync.Mutex
	id         int64
	coords     []osm.Node
	elem       *list.Element
	needsWrite bool
}

func (b *coordsBunch) GetCoord(id int64) (*osm.Node, error) {
	idx := sort.Search(len(b.coords), func(i int) bool {
		return b.coords[i].ID >= id
	})
	if idx < len(b.coords) && b.coords[idx].ID == id {
		nd := b.coords[idx] // create copy prevent to race when node gets reprojected
		return &nd, nil
	}
	return nil, NotFound
}

func (b *coordsBunch) DeleteCoord(id int64) {
	idx := sort.Search(len(b.coords), func(i int) bool {
		return b.coords[i].ID >= id
	})
	if idx < len(b.coords) && b.coords[idx].ID == id {
		b.coords = append(b.coords[:idx], b.coords[idx+1:]...)
	}
}

// PutCoord puts a single coord into the coords bunch. This function
// does support updating nodes.
func (b *coordsBunch) PutCoord(node osm.Node) {
	idx := sort.Search(len(b.coords), func(i int) bool {
		return b.coords[i].ID >= node.ID
	})
	if idx < len(b.coords) {
		if b.coords[idx].ID == node.ID {
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
func (b *coordsBunch) PutCoords(nodes []osm.Node) {
	b.coords = append(b.coords, nodes...)
	sort.Sort(byID(b.coords))
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

func (c *DeltaCoordsCache) SetLinearImport(v bool) {
	c.linearImport = v
}

func (c *DeltaCoordsCache) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for bunchID, bunch := range c.table {
		if bunch.needsWrite {
			err := c.putCoordsPacked(bunchID, bunch.coords)
			if err != nil {
				return err
			}
		}
	}

	c.lruList.Init()
	for k := range c.table {
		delete(c.table, k)
	}
	return nil
}
func (c *DeltaCoordsCache) Close() error {
	err := c.Flush()
	if err != nil {
		return err
	}
	c.cache.Close()
	return nil
}

func (c *DeltaCoordsCache) SetReadOnly(val bool) {
	c.readOnly = val
}

func (c *DeltaCoordsCache) GetCoord(id int64) (*osm.Node, error) {
	bunchID := c.getBunchID(id)
	bunch, err := c.getBunch(bunchID)
	if err != nil {
		return nil, err
	}
	if c.readOnly {
		bunch.Unlock()
	} else {
		defer bunch.Unlock()
	}
	return bunch.GetCoord(id)
}

func (c *DeltaCoordsCache) DeleteCoord(id int64) error {
	bunchID := c.getBunchID(id)
	bunch, err := c.getBunch(bunchID)
	if err != nil {
		return err
	}
	defer bunch.Unlock()
	bunch.DeleteCoord(id)
	bunch.needsWrite = true
	return nil
}

func (c *DeltaCoordsCache) FillWay(way *osm.Way) error {
	if way == nil {
		return nil
	}
	way.Nodes = make([]osm.Node, len(way.Refs))

	var err error
	var bunch *coordsBunch
	var bunchID, lastBunchID int64
	lastBunchID = -1

	for i, id := range way.Refs {
		bunchID = c.getBunchID(id)
		// re-use bunches
		if bunchID != lastBunchID {
			if bunch != nil {
				bunch.Unlock()
			}
			bunch, err = c.getBunch(bunchID)
			if err != nil {
				return err
			}
		}
		lastBunchID = bunchID

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

func removeSkippedNodes(nodes []osm.Node) []osm.Node {
	insertPoint := 0
	for i := 0; i < len(nodes); i++ {
		if i != insertPoint {
			nodes[insertPoint] = nodes[i]
		}
		if nodes[i].ID != SKIP {
			insertPoint++
		}
	}
	return nodes[:insertPoint]
}

// PutCoords puts nodes into cache.
// nodes need to be sorted by ID.
func (c *DeltaCoordsCache) PutCoords(nodes []osm.Node) error {
	var start, currentBunchID int64
	nodes = removeSkippedNodes(nodes)
	if len(nodes) == 0 {
		// skipped all nodes
		return nil
	}
	currentBunchID = c.getBunchID(nodes[0].ID)
	start = 0
	totalNodes := len(nodes)
	for i, node := range nodes {
		bunchID := c.getBunchID(node.ID)
		if bunchID != currentBunchID {
			if c.linearImport && int64(i) > c.bunchSize && int64(i) < int64(totalNodes)-c.bunchSize {
				// no need to handle concurrent updates to the same
				// bunch if we are not at the boundary of a c.bunchSize
				err := c.putCoordsPacked(currentBunchID, nodes[start:i])
				if err != nil {
					return err
				}
			} else {
				bunch, err := c.getBunch(currentBunchID)
				if err != nil {
					return err
				}
				if c.linearImport {
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
			currentBunchID = bunchID
			start = int64(i)
		}
	}
	bunch, err := c.getBunch(currentBunchID)
	if err != nil {
		return err
	}

	if c.linearImport {
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

func (c *DeltaCoordsCache) putCoordsPacked(bunchID int64, nodes []osm.Node) error {
	keyBuf := idToKeyBuf(bunchID)

	if len(nodes) == 0 {
		return c.db.Delete(keyBuf, c.wo)
	}

	data := make([]byte, 512)
	data = binary.MarshalDeltaNodes(nodes, data)

	err := c.db.Put(keyBuf, data, c.wo)
	if err != nil {
		return err
	}

	return nil
}

func (c *DeltaCoordsCache) getCoordsPacked(bunchID int64, nodes []osm.Node) ([]osm.Node, error) {
	keyBuf := idToKeyBuf(bunchID)

	data, err := c.db.Get(keyBuf, c.ro)
	if err != nil && err != leveldb.ErrNotFound {
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

func (c *DeltaCoordsCache) getBunchID(nodeID int64) int64 {
	return nodeID / c.bunchSize
}

func (c *DeltaCoordsCache) getBunch(bunchID int64) (*coordsBunch, error) {
	c.mu.Lock()
	bunch, ok := c.table[bunchID]
	var nodes []osm.Node
	needsGet := false
	if !ok {
		elem := c.lruList.PushFront(bunchID)
		nodes = make([]osm.Node, 0, c.bunchSize)
		bunch = &coordsBunch{id: bunchID, coords: nodes, elem: elem}
		needsGet = true
		c.table[bunchID] = bunch
	} else {
		c.lruList.MoveToFront(bunch.elem)
	}
	bunch.Lock()
	err := c.CheckCapacity()
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}

	if needsGet {
		nodes, err := c.getCoordsPacked(bunchID, nodes)
		if err != nil {
			return nil, err
		}
		bunch.coords = nodes
	}

	return bunch, nil
}

func (c *DeltaCoordsCache) CheckCapacity() error {
	for int64(len(c.table)) > c.capacity {
		elem := c.lruList.Back()
		bunchID := c.lruList.Remove(elem).(int64)
		bunch := c.table[bunchID]
		bunch.elem = nil
		if bunch.needsWrite {
			if err := c.putCoordsPacked(bunchID, bunch.coords); err != nil {
				return err
			}
		}
		delete(c.table, bunchID)
	}
	return nil
}

func (c *DeltaCoordsCache) FirstRefIsCached(refs []int64) (bool, error) {
	if len(refs) <= 0 {
		return false, nil
	}
	_, err := c.GetCoord(refs[0])
	if err == NotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
