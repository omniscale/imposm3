package binary

import (
	"bytes"
	"encoding/binary"
	"goposm/element"
)

func MarshalDeltaNodes(nodes []element.Node, buf []byte) []byte {
	estimatedLength := len(nodes)*4*3 + binary.MaxVarintLen64

	if len(buf) < estimatedLength {
		buf = make([]byte, estimatedLength)
	}

	lastId := int64(0)
	nextPos := binary.PutVarint(buf, int64(len(nodes)))

	for i := range nodes {
		if len(buf)-nextPos < binary.MaxVarintLen64 {
			tmp := make([]byte, len(buf)*3/2)
			copy(tmp, buf)
			buf = tmp
		}
		nextPos += binary.PutVarint(buf[nextPos:], nodes[i].Id-lastId)
		lastId = nodes[i].Id
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

func UnmarshalDeltaNodes(buf []byte, nodes []element.Node) ([]element.Node, error) {
	r := bytes.NewBuffer(buf)
	length, err := binary.ReadVarint(r)
	if err != nil {
		return nil, err
	}

	if int64(cap(nodes)) < length {
		nodes = make([]element.Node, length)
	} else {
		nodes = nodes[:length]
	}

	lastId := int64(0)
	for i := 0; int64(i) < length; i++ {
		id, err := binary.ReadVarint(r)
		if err != nil {
			return nil, err
		}
		id = lastId + id
		nodes[i].Id = id
		lastId = id
	}

	lastLong := int64(0)
	for i := 0; int64(i) < length; i++ {
		long, err := binary.ReadVarint(r)
		if err != nil {
			return nil, err
		}
		long = lastLong + long
		nodes[i].Long = IntToCoord(uint32(long))
		lastLong = long
	}

	lastLat := int64(0)
	for i := 0; int64(i) < length; i++ {
		lat, err := binary.ReadVarint(r)
		if err != nil {
			return nil, err
		}
		lat = lastLat + lat
		nodes[i].Lat = IntToCoord(uint32(lat))
		lastLat = lat
	}

	return nodes, nil
}
