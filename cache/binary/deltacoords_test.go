package binary

import (
	"math"
	"math/rand"
	"runtime"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/omniscale/imposm3/element"
)

type fataler interface {
	Fatalf(string, ...interface{})
}

func compareNodes(t fataler, a []element.Node, b []element.Node) {
	if len(a) != len(b) {
		t.Fatalf("length did not match %d != %d", len(a), len(b))
	}

	for i := range a {
		if a[i].Id != b[i].Id {
			t.Fatalf("id did not match %d != %d", a[i].Id, b[i].Id)
		}
		if math.Abs(a[i].Long-b[i].Long) > 1e-7 {
			t.Fatalf("long did not match %v != %v", a[i].Long, b[i].Long)
		}
		if math.Abs(a[i].Lat-b[i].Lat) > 1e-7 {
			t.Fatalf("lat did not match %v != %v", a[i].Lat, b[i].Lat)
		}
	}
}

var nodes []element.Node

func init() {
	nodes = make([]element.Node, 64)
	offset := rand.Int63n(1e10)
	for i := range nodes {
		nodes[i] = element.Node{OSMElem: element.OSMElem{Id: offset + rand.Int63n(1000)}, Long: rand.Float64()*360 - 180, Lat: rand.Float64()*180 - 90}
	}
}

func TestMarshalDeltaCoords(t *testing.T) {
	buf := MarshalDeltaNodes(nodes, nil)
	nodes2, _ := UnmarshalDeltaNodes(buf, nil)

	compareNodes(t, nodes, nodes2)
}

func BenchmarkMarshalDeltaCoords(b *testing.B) {
	var buf []byte

	for n := 0; n < b.N; n++ {
		buf = MarshalDeltaNodes(nodes, buf)
	}
	nodes2, _ := UnmarshalDeltaNodes(buf, nil)

	compareNodes(b, nodes, nodes2)
	runtime.GC()
}

func BenchmarkUnmarshalDeltaCoords(b *testing.B) {
	buf := MarshalDeltaNodes(nodes, nil)

	var nodes2 []element.Node
	for n := 0; n < b.N; n++ {
		nodes2, _ = UnmarshalDeltaNodes(buf, nodes2)
	}

	compareNodes(b, nodes, nodes2)
	runtime.GC()

}

func BenchmarkMarshalDeltaCoordsProto(b *testing.B) {
	var buf []byte
	var err error

	for n := 0; n < b.N; n++ {
		deltaCoords := packNodes(nodes)
		buf, err = proto.Marshal(deltaCoords)
		if err != nil {
			panic(err)
		}
	}

	deltaCoords := &DeltaCoords{}
	err = proto.Unmarshal(buf, deltaCoords)
	if err != nil {
		panic(err)
	}

	nodes2 := unpackNodes(deltaCoords, nodes)

	compareNodes(b, nodes, nodes2)
	runtime.GC()

}

func BenchmarkUnmarshalDeltaCoordsProto(b *testing.B) {
	var buf []byte
	var err error

	deltaCoords := packNodes(nodes)
	buf, err = proto.Marshal(deltaCoords)
	if err != nil {
		panic(err)
	}
	var nodes2 []element.Node
	for n := 0; n < b.N; n++ {
		deltaCoords := &DeltaCoords{}
		err = proto.Unmarshal(buf, deltaCoords)
		if err != nil {
			panic(err)
		}
		nodes2 = unpackNodes(deltaCoords, nodes)
	}
	compareNodes(b, nodes, nodes2)
	runtime.GC()

}

func packNodes(nodes []element.Node) *DeltaCoords {
	var lastLon, lastLat int64
	var lon, lat int64
	var lastId int64
	ids := make([]int64, len(nodes))
	lons := make([]int64, len(nodes))
	lats := make([]int64, len(nodes))

	i := 0
	for _, nd := range nodes {
		lon = int64(CoordToInt(nd.Long))
		lat = int64(CoordToInt(nd.Lat))
		ids[i] = nd.Id - lastId
		lons[i] = lon - lastLon
		lats[i] = lat - lastLat

		lastId = nd.Id
		lastLon = lon
		lastLat = lat
		i++
	}
	return &DeltaCoords{Ids: ids, Lats: lats, Lons: lons}
}

func unpackNodes(deltaCoords *DeltaCoords, nodes []element.Node) []element.Node {
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
			Long:    IntToCoord(uint32(lon)),
			Lat:     IntToCoord(uint32(lat)),
		}

		lastId = id
		lastLon = lon
		lastLat = lat
	}
	return nodes
}
