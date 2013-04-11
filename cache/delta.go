package cache

import (
	"goposm/binary"
	"goposm/element"
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
