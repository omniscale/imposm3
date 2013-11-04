package cache

import (
	"container/list"
	"imposm3/cache/binary"
	"imposm3/element"
	"sort"
	"sync"
)

type byId []element.Node

func (s byId) Len() int           { return len(s) }
func (s byId) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s byId) Less(i, j int) bool { return s[i].Id < s[j].Id }

func packNodes(nodes []element.Node) *binary.DeltaCoords {
	var lastLon, lastLat int64
	var lon, lat int64
	var lastId int64
	ids := make([]int64, len(nodes))
	lons := make([]int64, len(nodes))
	lats := make([]int64, len(nodes))

	i := 0
	for _, nd := range nodes {
		lon = int64(binary.CoordToInt(nd.Long))
		lat = int64(binary.CoordToInt(nd.Lat))
		ids[i] = nd.Id - lastId
		lons[i] = lon - lastLon
		lats[i] = lat - lastLat

		lastId = nd.Id
		lastLon = lon
		lastLat = lat
		i++
	}
	return &binary.DeltaCoords{Ids: ids, Lats: lats, Lons: lons}
}

func unpackNodes(deltaCoords *binary.DeltaCoords, nodes []element.Node) []element.Node {
	if len(deltaCoords.Ids) > cap(nodes) {
		nodes = make([]element.Node, len(deltaCoords.Ids))
	} else {
		nodes = nodes[:len(deltaCoords.Ids)]
	}

	var lastLon, lastLat int64
	var lon, lat int64
	var lastId, id int64

	for i := 0; i < len(deltaCoords.Ids); i++ {
		id = lastId + deltaCoords.Ids[i]
		lon = lastLon + deltaCoords.Lons[i]
		lat = lastLat + deltaCoords.Lats[i]
		nodes[i] = element.Node{
			OSMElem: element.OSMElem{Id: int64(id)},
			Long:    binary.IntToCoord(uint32(lon)),
			Lat:     binary.IntToCoord(uint32(lat)),
		}

		lastId = id
		lastLon = lon
		lastLat = lat
	}
	return nodes
}

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
		return &b.coords[idx], nil
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
	coordsCache.table = make(map[int64]*coordsBunch)
	// mem req for cache approx. capacity*bunchSize*40
	coordsCache.capacity = int64(globalCacheOptions.Coords.BunchCacheCapacity)
	return &coordsCache, nil
}

func (self *DeltaCoordsCache) SetLinearImport(v bool) {
	self.linearImport = v
}

func (self *DeltaCoordsCache) Flush() {
	for bunchId, bunch := range self.table {
		if bunch.needsWrite {
			self.putCoordsPacked(bunchId, bunch.coords)
		}
	}
}
func (self *DeltaCoordsCache) Close() {
	self.Flush()
	self.cache.Close()
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

// PutCoords puts nodes into cache.
// nodes need to be sorted by Id.
func (self *DeltaCoordsCache) PutCoords(nodes []element.Node) error {
	var start, currentBunchId int64
	currentBunchId = self.getBunchId(nodes[0].Id)
	start = 0
	totalNodes := len(nodes)
	for i, node := range nodes {
		if node.Id == SKIP {
			continue
		}
		bunchId := self.getBunchId(node.Id)
		if bunchId != currentBunchId {
			if self.linearImport && int64(i) > self.bunchSize && int64(i) < int64(totalNodes)-self.bunchSize {
				// no need to handle concurrent updates to the same
				// bunch if we are not at the boundary of a self.bunchSize
				self.putCoordsPacked(currentBunchId, nodes[start:i])
			} else {
				bunch, err := self.getBunch(currentBunchId)
				if err != nil {
					return err
				}
				bunch.coords = append(bunch.coords, nodes[start:i]...)
				sort.Sort(byId(bunch.coords))
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
	bunch.coords = append(bunch.coords, nodes[start:]...)
	sort.Sort(byId(bunch.coords))
	bunch.needsWrite = true
	bunch.Unlock()
	return nil
}

var (
	freeBuffer = make(chan []byte, 4)
)

func (p *DeltaCoordsCache) putCoordsPacked(bunchId int64, nodes []element.Node) error {
	keyBuf := idToKeyBuf(bunchId)

	if len(nodes) == 0 {
		return p.db.Delete(p.wo, keyBuf)
	}

	var data []byte
	select {
	case data = <-freeBuffer:
	default:
	}

	data = binary.MarshalDeltaNodes(nodes, data)

	err := p.db.Put(p.wo, keyBuf, data)
	if err != nil {
		return err
	}

	select {
	case freeBuffer <- data:
	default:
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

var (
	freeNodes = make(chan []element.Node, 4)
)

func (self *DeltaCoordsCache) getBunch(bunchId int64) (*coordsBunch, error) {
	self.mu.Lock()
	bunch, ok := self.table[bunchId]
	var nodes []element.Node
	needsGet := false
	if !ok {
		elem := self.lruList.PushFront(bunchId)
		select {
		case nodes = <-freeNodes:
		default:
			nodes = make([]element.Node, 0, self.bunchSize)
		}
		bunch = &coordsBunch{id: bunchId, coords: nil, elem: elem}
		needsGet = true
		self.table[bunchId] = bunch
	} else {
		self.lruList.MoveToFront(bunch.elem)
	}
	bunch.Lock()
	self.CheckCapacity()
	self.mu.Unlock()

	if needsGet {
		nodes, err := self.getCoordsPacked(bunchId, nodes)
		if err != nil {
			return nil, err
		}
		bunch.coords = nodes
	}

	return bunch, nil
}

func (self *DeltaCoordsCache) CheckCapacity() {
	for int64(len(self.table)) > self.capacity {
		elem := self.lruList.Back()
		bunchId := self.lruList.Remove(elem).(int64)
		bunch := self.table[bunchId]
		bunch.elem = nil
		if bunch.needsWrite {
			self.putCoordsPacked(bunchId, bunch.coords)
		}
		select {
		case freeNodes <- bunch.coords:
		default:
		}
		delete(self.table, bunchId)
	}
}
