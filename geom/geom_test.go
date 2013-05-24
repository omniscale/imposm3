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
	g := geos.NewGeos()
	defer g.Finish()
	geom, err := LineStringWkb(g, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(geom.Wkb[0:2], []byte{0x1, 0x2}) != 0 {
		t.Errorf("%#v", geom.Wkb)
	}
}

func TestPolygon(t *testing.T) {
	nodes := []element.Node{
		element.Node{Lat: 0, Long: 0},
		element.Node{Lat: 0, Long: 10},
		element.Node{Lat: 10, Long: 10},
		element.Node{Lat: 0, Long: 0},
	}
	g := geos.NewGeos()
	defer g.Finish()
	geom, err := PolygonWkb(g, nodes)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(geom.Wkb[0:2], []byte{0x1, 0x3}) != 0 {
		t.Errorf("%#v", geom.Wkb)
	}
}

func TestPolygonNotClosed(t *testing.T) {
	nodes := []element.Node{
		element.Node{Lat: 0, Long: 0},
		element.Node{Lat: 0, Long: 10},
		element.Node{Lat: 10, Long: 10},
	}
	g := geos.NewGeos()
	defer g.Finish()
	_, err := PolygonWkb(g, nodes)
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
	g := geos.NewGeos()
	defer g.Finish()

	for i := 0; i < b.N; i++ {
		LineStringWkb(g, nodes)
	}
}
