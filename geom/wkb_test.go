package geom

import (
	"strings"
	"testing"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/geom/geos"
)

func TestWkbLineString(t *testing.T) {
	nodes := make([]osm.Node, 5)
	nodes[0] = osm.Node{Lat: 0, Long: 0}
	nodes[1] = osm.Node{Lat: 1.123, Long: -0.2}
	nodes[2] = osm.Node{Lat: 1.99, Long: 1}
	nodes[3] = osm.Node{Lat: 0, Long: 1.1}
	nodes[4] = osm.Node{Lat: 0, Long: 0}
	g := geos.NewGeos()
	defer g.Finish()

	geom, err := LineString(g, nodes)
	if err != nil {
		t.Fatal(err)
	}
	geosWkb := string(g.AsEwkbHex(geom))
	wkbb, err := NodesAsEWKBHexLineString(nodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	wkb := strings.ToUpper(string(wkbb))

	if geosWkb != wkb {
		t.Error("linestring wkb differs")
		t.Error(string(geosWkb))
		t.Error(string(wkb))
	}
}

func TestWkbPolygon(t *testing.T) {
	nodes := make([]osm.Node, 5)
	nodes[0] = osm.Node{Lat: 1.123, Long: -0.2}
	nodes[1] = osm.Node{Lat: 1.99, Long: 1}
	nodes[2] = osm.Node{Lat: 0, Long: 1.1}
	nodes[3] = osm.Node{Lat: 0, Long: 0}
	nodes[4] = osm.Node{Lat: 1.123, Long: -0.2}
	g := geos.NewGeos()
	defer g.Finish()

	geom, err := Polygon(g, nodes)
	if err != nil {
		t.Fatal(err)
	}
	geosWkb := string(g.AsEwkbHex(geom))
	wkbb, err := NodesAsEWKBHexPolygon(nodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	wkb := strings.ToUpper(string(wkbb))

	if geosWkb != wkb {
		t.Error("polygon wkb differs")
		t.Error(string(geosWkb))
		t.Error(string(wkb))
	}
}

func BenchmarkAsWkb(b *testing.B) {
	g := geos.NewGeos()
	defer g.Finish()

	p := g.FromWkt("LINESTRING(0 0, 5 0, 10 0, 10 5, 10 10, 0 10, 0 0)")

	for i := 0; i < b.N; i++ {
		g.AsEwkbHex(p)
	}
}

func BenchmarkAsWkbSrid(b *testing.B) {
	g := geos.NewGeos()
	g.SetHandleSrid(4326)
	defer g.Finish()

	p := g.FromWkt("LINESTRING(0 0, 5 0, 10 0, 10 5, 10 10, 0 10, 0 0)")

	for i := 0; i < b.N; i++ {
		g.AsEwkbHex(p)
	}
}
