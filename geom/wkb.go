package geom

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"

	osm "github.com/omniscale/go-osm"
)

const (
	wkbSridFlag       = 0x20000000
	wkbLineStringType = 2
	wkbPolygonType    = 3
)

func NodesAsEWKBHexLineString(nodes []osm.Node, srid int) ([]byte, error) {
	nodes = unduplicateNodes(nodes)
	if len(nodes) < 2 {
		return nil, ErrorOneNodeWay
	}
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.LittleEndian, uint8(1)) // little endian
	if srid != 0 {
		binary.Write(buf, binary.LittleEndian, uint32(wkbLineStringType|wkbSridFlag))
		binary.Write(buf, binary.LittleEndian, uint32(srid))
	} else {
		binary.Write(buf, binary.LittleEndian, uint32(wkbLineStringType))
	}
	binary.Write(buf, binary.LittleEndian, uint32(len(nodes)))

	for _, nd := range nodes {
		binary.Write(buf, binary.LittleEndian, nd.Long)
		binary.Write(buf, binary.LittleEndian, nd.Lat)
	}

	src := buf.Bytes()
	dst := make([]byte, hex.EncodedLen(len(src)))
	hex.Encode(dst, src)
	return dst, nil
}

func NodesAsEWKBHexPolygon(nodes []osm.Node, srid int) ([]byte, error) {
	// TODO undup nodes and check if closed
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.LittleEndian, uint8(1)) // little endian
	if srid != 0 {
		binary.Write(buf, binary.LittleEndian, uint32(wkbPolygonType|wkbSridFlag))
		binary.Write(buf, binary.LittleEndian, uint32(srid))
	} else {
		binary.Write(buf, binary.LittleEndian, uint32(wkbPolygonType))
	}
	binary.Write(buf, binary.LittleEndian, uint32(1)) // one ring
	binary.Write(buf, binary.LittleEndian, uint32(len(nodes)))

	for _, nd := range nodes {
		binary.Write(buf, binary.LittleEndian, nd.Long)
		binary.Write(buf, binary.LittleEndian, nd.Lat)
	}

	src := buf.Bytes()
	dst := make([]byte, hex.EncodedLen(len(src)))
	hex.Encode(dst, src)
	return dst, nil
}
