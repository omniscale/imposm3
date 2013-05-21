package cache

import (
	"container/list"
	"goposm/cache/binary"
	"goposm/element"
	"sort"
	"sync"
)

type Nodes []element.Node

func (s Nodes) Len() int           { return len(s) }
func (s Nodes) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Nodes) Less(i, j int) bool { return s[i].Id < s[j].Id }

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

type CoordsBunch struct {
	sync.Mutex
	id         int64
	coords     []element.Node
	elem       *list.Element
	needsWrite bool
}

func (b *CoordsBunch) GetCoord(id int64) (*element.Node, error) {
	idx := sort.Search(len(b.coords), func(i int) bool {
		return b.coords[i].Id >= id
	})
	if idx < len(b.coords) && b.coords[idx].Id == id {
		return &b.coords[idx], nil
	}
	return nil, NotFound
}

type DeltaCoordsCache struct {
	Cache
	lruList      *list.List
	table        map[int64]*CoordsBunch
	capacity     int64
	linearImport bool
	mu           sync.Mutex
}

func NewDeltaCoordsCache(path string) (*DeltaCoordsCache, error) {
	coordsCache := DeltaCoordsCache{}
	err := coordsCache.open(path)
	if err != nil {
		return nil, err
	}
	coordsCache.lruList = list.New()
	coordsCache.table = make(map[int64]*CoordsBunch)
	// mem req for cache approx. capacity*deltaCacheBunchSize*30
	coordsCache.capacity = 1024 * 8
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
	self.Cache.Close()
}

func (self *DeltaCoordsCache) GetCoord(id int64) (*element.Node, error) {
	bunchId := getBunchId(id)
	bunch, err := self.getBunch(bunchId)
	if err != nil {
		return nil, err
	}
	defer bunch.Unlock()
	return bunch.GetCoord(id)
}

func (self *DeltaCoordsCache) FillWay(way *element.Way) error {
	if way == nil {
		return nil
	}
	way.Nodes = make([]element.Node, len(way.Refs))

	var err error
	var bunch *CoordsBunch
	var bunchId, lastBunchId int64
	lastBunchId = -1

	for i, id := range way.Refs {
		bunchId = getBunchId(id)
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
	currentBunchId = getBunchId(nodes[0].Id)
	start = 0
	totalNodes := len(nodes)
	for i, node := range nodes {
		bunchId := getBunchId(node.Id)
		if bunchId != currentBunchId {
			if self.linearImport && int64(i) > deltaCacheBunchSize && int64(i) < int64(totalNodes)-deltaCacheBunchSize {
				// no need to handle concurrent updates to the same
				// bunch if we are not at the boundary of a deltaCacheBunchSize
				self.putCoordsPacked(currentBunchId, nodes[start:i])
			} else {
				bunch, err := self.getBunch(currentBunchId)
				if err != nil {
					return err
				}
				bunch.coords = append(bunch.coords, nodes[start:i]...)
				sort.Sort(Nodes(bunch.coords))
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
	sort.Sort(Nodes(bunch.coords))
	bunch.needsWrite = true
	bunch.Unlock()
	return nil
}

func (p *DeltaCoordsCache) putCoordsPacked(bunchId int64, nodes []element.Node) error {
	if len(nodes) == 0 {
		return nil
	}
	keyBuf := idToKeyBuf(bunchId)

	data := binary.MarshalDeltaNodes(nodes, nil)

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

func getBunchId(nodeId int64) int64 {
	return nodeId / deltaCacheBunchSize
}

var (
	freeNodes = make(chan []element.Node, 4)
)

func (self *DeltaCoordsCache) getBunch(bunchId int64) (*CoordsBunch, error) {
	self.mu.Lock()
	defer self.mu.Unlock()
	bunch, ok := self.table[bunchId]
	var nodes []element.Node
	if !ok {
		elem := self.lruList.PushFront(bunchId)
		select {
		case nodes = <-freeNodes:
		default:
			nodes = make([]element.Node, 0, deltaCacheBunchSize)
		}
		nodes, err := self.getCoordsPacked(bunchId, nodes)
		if err != nil {
			return nil, err
		}
		bunch = &CoordsBunch{id: bunchId, coords: nodes, elem: elem}
		self.table[bunchId] = bunch
	} else {
		self.lruList.MoveToFront(bunch.elem)
	}
	bunch.Lock()
	self.CheckCapacity()
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
