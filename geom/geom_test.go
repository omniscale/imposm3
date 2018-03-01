package geom

import (
	"testing"

	"github.com/gregtzar/imposm3/element"
	"github.com/gregtzar/imposm3/geom/geos"
)

func TestLineString(t *testing.T) {
	nodes := make([]element.Node, 2)
	nodes[0] = element.Node{Lat: 0, Long: 0}
	nodes[1] = element.Node{Lat: 0, Long: 10}
	g := geos.NewGeos()
	defer g.Finish()
	geom, err := LineString(g, nodes)
	if err != nil {
		t.Fatal(err)
	}

	if geom.Length() != 10.0 {
		t.Fatal(geom.Length())
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
	geom, err := Polygon(g, nodes)
	if err != nil {
		t.Fatal(err)
	}

	if geom.Area() != 50.0 {
		t.Fatal(geom.Area())
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
	_, err := Polygon(g, nodes)
	if err == nil {
		t.Fatal("no error")
	}
}

func TestPolygonIntersection(t *testing.T) {
	nodes := []element.Node{
		element.Node{Lat: 0, Long: 0},
		element.Node{Lat: 0, Long: 10},
		element.Node{Lat: 10, Long: 10},
		element.Node{Lat: 10, Long: 0},
		element.Node{Lat: 0, Long: 0},
	}
	g := geos.NewGeos()
	defer g.Finish()
	geom, err := Polygon(g, nodes)
	if err != nil {
		t.Fatal(err)
	}

	result := g.Intersection(geom, g.FromWkt("LINESTRING(-10 5, 20 5)"))

	if !g.Equals(result, g.FromWkt("LINESTRING(0 5, 10 5)")) {
		t.Fatal(g.AsWkt(result))
	}
}

func BenchmarkLineString(b *testing.B) {
	size := 16
	nodes := make([]element.Node, size)
	for i := 0; i < size; i++ {
		nodes[i] = element.Node{Lat: 0, Long: float64(i)}
	}
	g := geos.NewGeos()
	g.SetHandleSrid(4326)
	defer g.Finish()

	for i := 0; i < b.N; i++ {
		geom, err := LineString(g, nodes)
		if err != nil {
			b.Fatal(err)
		}
		g.AsEwkbHex(geom)
	}
}

func BenchmarkLineStringNoGeos(b *testing.B) {
	size := 16
	nodes := make([]element.Node, size)
	for i := 0; i < size; i++ {
		nodes[i] = element.Node{Lat: 0, Long: float64(i)}
	}

	for i := 0; i < b.N; i++ {
		NodesAsEWKBHexLineString(nodes, 4326)
	}
}

func BenchmarkPolygon(b *testing.B) {
	size := 16
	nodes := make([]element.Node, size)
	for i := 1; i < size; i++ {
		nodes[i] = element.Node{Lat: 1, Long: float64(i)}
	}
	nodes[0] = element.Node{Lat: 0, Long: 0}
	nodes[len(nodes)-1] = element.Node{Lat: 0, Long: 0}
	g := geos.NewGeos()
	g.SetHandleSrid(4326)
	defer g.Finish()

	for i := 0; i < b.N; i++ {
		geom, err := Polygon(g, nodes)
		if err != nil {
			b.Fatal(err)
		}
		g.AsEwkbHex(geom)
	}
}

func BenchmarkPolygonNoGeos(b *testing.B) {
	size := 16
	nodes := make([]element.Node, size)
	for i := 1; i < size; i++ {
		nodes[i] = element.Node{Lat: 1, Long: float64(i)}
	}
	nodes[0] = element.Node{Lat: 0, Long: 0}
	nodes[len(nodes)-1] = element.Node{Lat: 0, Long: 0}

	for i := 0; i < b.N; i++ {
		NodesAsEWKBHexPolygon(nodes, 4326)
	}
}

func TestUnduplicateNodes(t *testing.T) {
	var nodes []element.Node

	nodes = []element.Node{
		element.Node{Lat: 0, Long: 0},
	}
	if res := unduplicateNodes(nodes); len(res) != 1 {
		t.Fatal(res)
	}
	nodes = []element.Node{
		element.Node{Lat: 47.0, Long: 80.0},
		element.Node{Lat: 47.0, Long: 80.0},
	}
	if res := unduplicateNodes(nodes); len(res) != 1 {
		t.Fatal(res)
	}

	nodes = []element.Node{
		element.Node{Lat: 0, Long: -10},
		element.Node{Lat: 0, Long: -10},
		element.Node{Lat: 0, Long: -10},
		element.Node{Lat: 10, Long: 10},
		element.Node{Lat: 10, Long: 10},
		element.Node{Lat: 10, Long: 10},
	}
	if res := unduplicateNodes(nodes); len(res) != 2 {
		t.Fatal(res)
	}

	nodes = []element.Node{
		element.Node{Lat: 10, Long: 10},
		element.Node{Lat: 0, Long: 10},
		element.Node{Lat: 10, Long: 10},
		element.Node{Lat: 10, Long: 10},
		element.Node{Lat: 0, Long: 10},
		element.Node{Lat: 0, Long: 10},
	}
	if res := unduplicateNodes(nodes); len(res) != 4 {
		t.Fatal(res)
	}

	nodes = []element.Node{
		element.Node{Lat: 0, Long: 0},
		element.Node{Lat: 0, Long: -10},
		element.Node{Lat: 10, Long: -10},
		element.Node{Lat: 10, Long: 0},
		element.Node{Lat: 0, Long: 0},
	}
	if res := unduplicateNodes(nodes); len(res) != 5 {
		t.Fatal(res)
	}

}
