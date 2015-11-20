package binary

import (
	"math"
	"math/rand"
	"runtime"
	"testing"

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
