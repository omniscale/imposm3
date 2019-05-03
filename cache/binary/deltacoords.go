package binary

import (
	"encoding/binary"
	"errors"

	osm "github.com/omniscale/go-osm"
)

func MarshalDeltaNodes(nodes []osm.Node, buf []byte) []byte {
	estimatedLength := len(nodes)*4*3 + binary.MaxVarintLen64

	if cap(buf) < estimatedLength {
		buf = make([]byte, estimatedLength)
	} else {
		// resize slice to full capacity
		buf = buf[:cap(buf)-1]
	}

	lastID := int64(0)
	nextPos := binary.PutUvarint(buf, uint64(len(nodes)))

	for i := range nodes {
		if len(buf)-nextPos < binary.MaxVarintLen64 {
			tmp := make([]byte, len(buf)*3/2)
			copy(tmp, buf)
			buf = tmp
		}
		nextPos += binary.PutVarint(buf[nextPos:], nodes[i].ID-lastID)
		lastID = nodes[i].ID
	}

	lastLong := int64(0)
	for i := range nodes {
		if len(buf)-nextPos < binary.MaxVarintLen64 {
			tmp := make([]byte, len(buf)*3/2)
			copy(tmp, buf)
			buf = tmp
		}
		long := int64(CoordToInt(nodes[i].Long))
		nextPos += binary.PutVarint(buf[nextPos:], long-lastLong)
		lastLong = long
	}

	lastLat := int64(0)
	for i := range nodes {
		if len(buf)-nextPos < binary.MaxVarintLen64 {
			tmp := make([]byte, len(buf)*3/2)
			copy(tmp, buf)
			buf = tmp
		}
		lat := int64(CoordToInt(nodes[i].Lat))
		nextPos += binary.PutVarint(buf[nextPos:], lat-lastLat)
		lastLat = lat
	}

	return buf[:nextPos]
}

var errVarInt = errors.New("unmarshal delta coords: missing data for varint or overflow")

func UnmarshalDeltaNodes(buf []byte, nodes []osm.Node) ([]osm.Node, error) {
	length, n := binary.Uvarint(buf)
	if n <= 0 {
		return nil, errVarInt
	}
	var offset = n

	if uint64(cap(nodes)) < length {
		nodes = make([]osm.Node, length)
	} else {
		nodes = nodes[:length]
	}

	lastID := int64(0)
	for i := 0; uint64(i) < length; i++ {
		id, n := binary.Varint(buf[offset:])
		if n <= 0 {
			return nil, errVarInt
		}
		offset += n
		id = lastID + id
		nodes[i].ID = id
		lastID = id
	}

	lastLong := int64(0)
	for i := 0; uint64(i) < length; i++ {
		long, n := binary.Varint(buf[offset:])
		if n <= 0 {
			return nil, errVarInt
		}
		offset += n
		long = lastLong + long
		nodes[i].Long = IntToCoord(uint32(long))
		lastLong = long
	}

	lastLat := int64(0)
	for i := 0; uint64(i) < length; i++ {
		lat, n := binary.Varint(buf[offset:])
		if n <= 0 {
			return nil, errVarInt
		}
		offset += n
		lat = lastLat + lat
		nodes[i].Lat = IntToCoord(uint32(lat))
		lastLat = lat
	}

	return nodes, nil
}
