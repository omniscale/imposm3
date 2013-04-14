package cache

import (
	"container/list"
	"goposm/binary"
	"goposm/element"
	"sync"
)

func packNodes(nodes []element.Node) *DeltaCoords {
	var lastLon, lastLat int64
	var lon, lat int64
	var lastId, id int64
	ids := make([]int64, len(nodes))
	lons := make([]int64, len(nodes))
	lats := make([]int64, len(nodes))

	for i, nd := range nodes {
		id = nd.Id
		lon = int64(binary.CoordToInt(nd.Long))
		lat = int64(binary.CoordToInt(nd.Lat))
		ids[i] = id - lastId
		lons[i] = lon - lastLon
		lats[i] = lat - lastLat

		lastId = id
		lastLon = lon
		lastLat = lat
	}
	return &DeltaCoords{Ids: ids, Lats: lats, Lons: lons}
}

func unpackNodes(deltaCoords *DeltaCoords) []element.Node {
	nodes := make([]element.Node, len(deltaCoords.Ids))

	var lastLon, lastLat int64
	var lon, lat int64
	var lastId, id int64

	for i := range nodes {
		id = lastId + deltaCoords.Ids[i]
		lon = lastLon + deltaCoords.Lats[i]
		lat = lastLat + deltaCoords.Lons[i]
		nodes[i].Id = id
		nodes[i].Long = binary.IntToCoord(uint32(lon))
		nodes[i].Lat = binary.IntToCoord(uint32(lat))

		lastId = id
		lastLon = lon
		lastLat = lat
	}
	return nodes
}

/*
type DeltaCoordsCache struct {
	list     *list.List
	table    map[int64]*list.Item
	capacity uint64
}

func (d *DeltaCoordsCache) Get(coordId int64) {
	coords, ok := d.table[coordId]
	if !ok {
		return nil
	}

}

func (d *DeltaCoordsCache) Add(coords *DeltaCoords) {
	id := coords[0].Id
	id = id / 8196
}

type LRUCache struct {
	mu sync.Mutex

	// list & table of *entry objects
	list  *list.List
	table map[int64]*list.Element

	// Our current size, in bytes. Obviously a gross simplification and low-grade
	// approximation.
	size uint64

	// How many bytes we are limiting the cache to.
	capacity uint64
}

type Item struct {
	Key   int64
	Value *list.Element
}

func NewLRUCache(capacity uint64) *LRUCache {
	return &LRUCache{
		list:     list.New(),
		table:    make(map[int64]*list.Element),
		capacity: capacity,
	}
}

func (self *LRUCache) Get(key int64) (v interface{}, ok bool) {
	self.mu.Lock()
	defer self.mu.Unlock()

	element := self.table[key]
	if element == nil {
		return nil, false
	}
	self.moveToFront(element)
	return element.Value, true
}

func (self *LRUCache) Set(id int64, value interface{}) {
	self.mu.Lock()
	defer self.mu.Unlock()

	if element := self.table[id]; element != nil {
		self.list.MoveToFront(element)
	} else {
		self.addNew(id, value)
	}
}

func (self *LRUCache) SetIfAbsent(key string, value Value) {
	self.mu.Lock()
	defer self.mu.Unlock()

	if element := self.table[key]; element != nil {
		self.moveToFront(element)
	} else {
		self.addNew(key, value)
	}
}

func (self *LRUCache) Delete(key int64) bool {
	self.mu.Lock()
	defer self.mu.Unlock()

	element := self.table[key]
	if element == nil {
		return false
	}

	self.list.Remove(element)
	delete(self.table, key)
	return true
}

func (self *LRUCache) Clear() {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.list.Init()
	self.table = make(map[string]*list.Element)
	self.size = 0
}

func (self *LRUCache) SetCapacity(capacity uint64) {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.capacity = capacity
	self.checkCapacity()
}

func (self *LRUCache) addNew(key string, value Value) {
	newEntry := &entry{key, value, value.Size(), time.Now()}
	element := self.list.PushFront(newEntry)
	self.table[key] = element
	self.size += uint64(newEntry.size)
	self.checkCapacity()
}

func (self *LRUCache) checkCapacity() {
	// Partially duplicated from Delete
	for len(self.table) > self.capacity {
		delElem := self.list.Back()
		delValue := delElem.Value.(*entry)
		self.list.Remove(delElem)
		delete(self.table, delValue.key)
		self.size -= uint64(delValue.size)
	}
}
*/

type CoordsBunchLRU struct {
	list     *list.List
	table    map[int64]*CoordsBunch
	capacity int64
	mu       sync.Mutex
}

func NewCoordsBunchLRU(capacity int64) *CoordsBunchLRU {
	lru := CoordsBunchLRU{}
	lru.list = list.New()
	lru.table = make(map[int64]*CoordsBunch)
	lru.capacity = capacity
	return &lru
}

type CoordsBunch struct {
	id     int64
	coords []element.Node
	mu     sync.RWMutex
}

type CoordsCache struct {
	cache *Cache
	lru   *CoordsBunchLRU
}

func NewCoordsCache(cache *Cache) *CoordsCache {
	coordsCache := CoordsCache{cache: cache}
	coordsCache.lru = NewCoordsBunchLRU(100)
	return &coordsCache
}

func (self *CoordsCache) Close() {
	for bunchId, bunch := range self.lru.table {
		self.cache.PutCoordsPacked(bunchId, bunch.coords)
	}
	self.cache.Close()
}
func (self *CoordsCache) PutCoord(node element.Node) {
	bunch := self.GetBunch(node.Id)
	bunch.mu.Lock()
	defer bunch.mu.Unlock()
	bunch.coords = append(bunch.coords, node)
}

func (self *CoordsCache) PutCoords(nodes []element.Node) {
	var start, currentBunchId int64
	currentBunchId = BunchId(nodes[0].Id)
	start = 0
	for i, node := range nodes {
		bunchId := BunchId(node.Id)
		if bunchId != currentBunchId {
			bunch := self.GetBunch(currentBunchId)
			bunch.coords = append(bunch.coords, nodes[start:i-1]...)
			currentBunchId = bunchId
			start = int64(i)
		}
	}
	bunch := self.GetBunch(currentBunchId)
	bunch.coords = append(bunch.coords, nodes[start:]...)
}

func BunchId(nodeId int64) int64 {
	return nodeId / (1024 * 128)
}

func (self *CoordsCache) GetBunch(bunchId int64) *CoordsBunch {
	self.lru.mu.Lock()
	defer self.lru.mu.Unlock()
	bunch, ok := self.lru.table[bunchId]
	if !ok {
		nodes := self.cache.GetCoordsPacked(bunchId)
		if nodes == nil {
			bunch = &CoordsBunch{}
		} else {
			bunch = &CoordsBunch{id: bunchId, coords: nodes}
		}
		self.lru.table[bunchId] = bunch
	}
	return bunch
}
