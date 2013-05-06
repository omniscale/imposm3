package geom

import (
	"bytes"
	"goposm/element"
	"goposm/geom/geos"
	"testing"
)

func TestLineString(t *testing.T) {
	nodes := make([]element.Node, 2)
	nodes[0] = element.Node{Lat: 0, Long: 0}
	nodes[1] = element.Node{Lat: 0, Long: 10}
	g := geos.NewGEOS()
	defer g.Finish()
	wkt, err := LineStringWKB(g, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(wkt[0:2], []byte{0x1, 0x2}) != 0 {
		t.Errorf("%#v", wkt)
	}
}

func TestPolygon(t *testing.T) {
	nodes := []element.Node{
		element.Node{Lat: 0, Long: 0},
		element.Node{Lat: 0, Long: 10},
		element.Node{Lat: 10, Long: 10},
		element.Node{Lat: 0, Long: 0},
	}
	g := geos.NewGEOS()
	defer g.Finish()
	wkt, err := PolygonWKB(g, nodes)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(wkt[0:2], []byte{0x1, 0x3}) != 0 {
		t.Errorf("%#v", wkt)
	}
}

func TestPolygonNotClosed(t *testing.T) {
	nodes := []element.Node{
		element.Node{Lat: 0, Long: 0},
		element.Node{Lat: 0, Long: 10},
		element.Node{Lat: 10, Long: 10},
	}
	g := geos.NewGEOS()
	defer g.Finish()
	_, err := PolygonWKB(g, nodes)
	if err == nil {
		t.Fatal("no error")
	}
}

func BenchmarkLineString(b *testing.B) {
	size := 16
	nodes := make([]element.Node, size)
	for i := 0; i < size; i++ {
		nodes[i] = element.Node{Lat: 0, Long: float64(i)}
	}
	g := geos.NewGEOS()
	defer g.Finish()

	for i := 0; i < b.N; i++ {
		LineStringWKB(g, nodes)
	}
}
