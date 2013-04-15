package cache

import (
	"code.google.com/p/goprotobuf/proto"
	"container/list"
	bin "encoding/binary"
	"goposm/binary"
	"goposm/element"
	"sync"
)

func packNodes(nodes map[int64]element.Node) *DeltaCoords {
	var lastLon, lastLat int64
	var lon, lat int64
	var lastId int64
	ids := make([]int64, len(nodes))
	lons := make([]int64, len(nodes))
	lats := make([]int64, len(nodes))

	i := 0
	for id, nd := range nodes {
		lon = int64(binary.CoordToInt(nd.Long))
		lat = int64(binary.CoordToInt(nd.Lat))
		ids[i] = id - lastId
		lons[i] = lon - lastLon
		lats[i] = lat - lastLat

		lastId = id
		lastLon = lon
		lastLat = lat
		i++
	}
	return &DeltaCoords{Ids: ids, Lats: lats, Lons: lons}
}

func unpackNodes(deltaCoords *DeltaCoords) map[int64]element.Node {
	nodes := make(map[int64]element.Node, len(deltaCoords.Ids))

	var lastLon, lastLat int64
	var lon, lat int64
	var lastId, id int64

	for i := 0; i < len(deltaCoords.Ids); i++ {
		id = lastId + deltaCoords.Ids[i]
		lon = lastLon + deltaCoords.Lats[i]
		lat = lastLat + deltaCoords.Lons[i]
		nodes[id] = element.Node{
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
	coords     map[int64]element.Node
	elem       *list.Element
	needsWrite bool
}

type DeltaCoordsCache struct {
	Cache
	lruList  *list.List
	table    map[int64]*CoordsBunch
	capacity int64
	mu       sync.Mutex
}

func NewDeltaCoordsCache(path string) *DeltaCoordsCache {
	cache := NewCache(path)
	coordsCache := DeltaCoordsCache{}
	coordsCache.Cache = cache
	coordsCache.lruList = list.New()
	coordsCache.table = make(map[int64]*CoordsBunch)
	coordsCache.capacity = 100
	return &coordsCache
}

func (self *DeltaCoordsCache) Close() {
	for getBunchId, bunch := range self.table {
		if bunch.needsWrite {
			self.putCoordsPacked(getBunchId, bunch.coords)
		}
	}
	self.Cache.Close()
}

func (self *DeltaCoordsCache) GetCoord(id int64) (element.Node, bool) {
	getBunchId := getBunchId(id)
	bunch := self.getBunch(getBunchId)
	defer bunch.Unlock()
	node, ok := bunch.coords[id]
	if !ok {
		return element.Node{}, false
	}
	return node, true
}

func (self *DeltaCoordsCache) PutCoords(nodes []element.Node) {
	var start, currentgetBunchId int64
	currentgetBunchId = getBunchId(nodes[0].Id)
	start = 0
	for i, node := range nodes {
		getBunchId := getBunchId(node.Id)
		if getBunchId != currentgetBunchId {
			bunch := self.getBunch(currentgetBunchId)
			for _, nd := range nodes[start : i-1] {
				bunch.coords[nd.Id] = nd
			}
			currentgetBunchId = getBunchId
			start = int64(i)
			bunch.needsWrite = true
			bunch.Unlock()
		}
	}
	bunch := self.getBunch(currentgetBunchId)
	for _, nd := range nodes[start:] {
		bunch.coords[nd.Id] = nd
	}
	bunch.needsWrite = true
	bunch.Unlock()
}

func (p *DeltaCoordsCache) putCoordsPacked(getBunchId int64, nodes map[int64]element.Node) {
	if len(nodes) == 0 {
		return
	}
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, getBunchId)

	deltaCoords := packNodes(nodes)
	data, err := proto.Marshal(deltaCoords)
	if err != nil {
		panic(err)
	}
	p.db.Put(p.wo, keyBuf, data)
}

func (p *DeltaCoordsCache) getCoordsPacked(getBunchId int64) map[int64]element.Node {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, getBunchId)

	data, err := p.db.Get(p.ro, keyBuf)
	if err != nil {
		panic(err)
	}
	deltaCoords := &DeltaCoords{}
	err = proto.Unmarshal(data, deltaCoords)
	if err != nil {
		panic(err)
	}

	nodes := unpackNodes(deltaCoords)
	return nodes
}

func getBunchId(nodeId int64) int64 {
	return nodeId / (1024 * 32)
}

func (self *DeltaCoordsCache) getBunch(getBunchId int64) *CoordsBunch {
	self.mu.Lock()
	defer self.mu.Unlock()
	bunch, ok := self.table[getBunchId]
	if !ok {
		elem := self.lruList.PushFront(getBunchId)
		nodes := self.getCoordsPacked(getBunchId)
		if nodes == nil {
			bunch = &CoordsBunch{elem: elem}
		} else {
			bunch = &CoordsBunch{id: getBunchId, coords: nodes, elem: elem}
		}
		self.table[getBunchId] = bunch
	} else {
		self.lruList.MoveToFront(bunch.elem)
	}
	bunch.Lock()
	self.CheckCapacity()
	return bunch
}

func (self *DeltaCoordsCache) CheckCapacity() {
	for int64(len(self.table)) > self.capacity {
		elem := self.lruList.Back()
		getBunchId := self.lruList.Remove(elem).(int64)
		bunch := self.table[getBunchId]
		if bunch.needsWrite {
			self.putCoordsPacked(getBunchId, bunch.coords)
		}
		delete(self.table, getBunchId)
	}
}
