package geom

import (
	"goposm/element"
	"goposm/geom/geos"
	"testing"
)

type coord struct {
	id   int64
	long float64
	lat  float64
}

func makeWay(id int64, tags element.Tags, coords []coord) element.Way {
	way := element.Way{}
	way.Id = id
	for _, coord := range coords {
		way.Refs = append(way.Refs, coord.id)
		way.Nodes = append(way.Nodes,
			element.Node{OSMElem: element.OSMElem{Id: coord.id}, Long: coord.long, Lat: coord.lat})
	}
	return way
}

func TestMultiPolygonWithHole(t *testing.T) {
	w1 := makeWay(1, nil, []coord{
		{1, 0, 0},
		{2, 10, 0},
		{3, 10, 10},
		{4, 0, 10},
		{1, 0, 0},
	})
	w2 := makeWay(1, nil, []coord{
		{5, 2, 2},
		{6, 8, 2},
		{7, 8, 8},
		{8, 2, 8},
		{5, 2, 2},
	})

	rel := element.Relation{OSMElem: element.OSMElem{Id: 1}}
	rel.Members = []element.Member{
		{1, element.WAY, "outer", &w1},
		{2, element.WAY, "inner", &w2},
	}

	BuildRelation(&rel)
	g := geos.NewGEOS()
	defer g.Finish()

	if !g.IsValid(rel.Geom.Geom) {
		t.Fatal("geometry not valid", g.AsWKT(rel.Geom.Geom))
	}

	if area := rel.Geom.Geom.Area(); area != 64 {
		t.Fatal("aread not 64", area)
	}
}

func TestMultiPolygonWithMultipleHoles(t *testing.T) {
	w1 := makeWay(1, nil, []coord{
		{1, 0, 0},
		{2, 10, 0},
		{3, 10, 10},
		{4, 0, 10},
		{1, 0, 0},
	})
	w2 := makeWay(1, nil, []coord{
		{1, 1, 1},
		{2, 2, 1},
		{3, 2, 2},
		{4, 1, 2},
		{1, 1, 1},
	})
	w3 := makeWay(3, nil, []coord{
		{1, 3, 3},
		{2, 4, 3},
		{3, 4, 4},
		{4, 3, 4},
		{1, 3, 3},
	})

	rel := element.Relation{OSMElem: element.OSMElem{Id: 1}}
	rel.Members = []element.Member{
		{1, element.WAY, "outer", &w1},
		{2, element.WAY, "inner", &w2},
		{3, element.WAY, "inner", &w3},
	}

	BuildRelation(&rel)
	g := geos.NewGEOS()
	defer g.Finish()

	if !g.IsValid(rel.Geom.Geom) {
		t.Fatal("geometry not valid", g.AsWKT(rel.Geom.Geom))
	}

	if area := rel.Geom.Geom.Area(); area != 100-1-1 {
		t.Fatal("area invalid", area)
	}
}

func TestMultiPolygonWithNeastedHoles(t *testing.T) {
	w1 := makeWay(1, nil, []coord{
		{1, 0, 0},
		{2, 10, 0},
		{3, 10, 10},
		{4, 0, 10},
		{1, 0, 0},
	})
	w2 := makeWay(2, nil, []coord{
		{1, 1, 1},
		{2, 9, 1},
		{3, 9, 9},
		{4, 1, 9},
		{1, 1, 1},
	})
	w3 := makeWay(3, nil, []coord{
		{1, 2, 2},
		{2, 8, 2},
		{3, 8, 8},
		{4, 2, 8},
		{1, 2, 2},
	})
	w4 := makeWay(4, nil, []coord{
		{1, 3, 3},
		{2, 7, 3},
		{3, 7, 7},
		{4, 3, 7},
		{1, 3, 3},
	})
	w5 := makeWay(5, nil, []coord{
		{1, 4, 4},
		{2, 6, 4},
		{3, 6, 6},
		{4, 4, 6},
		{1, 4, 4},
	})

	rel := element.Relation{OSMElem: element.OSMElem{Id: 1}}
	rel.Members = []element.Member{
		{1, element.WAY, "outer", &w1},
		{2, element.WAY, "inner", &w2},
		{3, element.WAY, "inner", &w3},
		{4, element.WAY, "inner", &w4},
		{5, element.WAY, "inner", &w5},
	}

	BuildRelation(&rel)
	g := geos.NewGEOS()
	defer g.Finish()

	if !g.IsValid(rel.Geom.Geom) {
		t.Fatal("geometry not valid", g.AsWKT(rel.Geom.Geom))
	}

	if area := rel.Geom.Geom.Area(); area != 100-64+36-16+4 {
		t.Fatal("area invalid", area)
	}
}

func TestPolygonFromTwoWays(t *testing.T) {
	w1 := makeWay(1, nil, []coord{
		{1, 0, 0},
		{2, 10, 0},
		{3, 10, 10},
	})
	w2 := makeWay(2, nil, []coord{
		{3, 10, 10},
		{4, 0, 10},
		{1, 0, 0},
	})

	rel := element.Relation{OSMElem: element.OSMElem{Id: 1}}
	rel.Members = []element.Member{
		{1, element.WAY, "outer", &w1},
		{2, element.WAY, "inner", &w2},
	}

	BuildRelation(&rel)
	g := geos.NewGEOS()
	defer g.Finish()

	if !g.IsValid(rel.Geom.Geom) {
		t.Fatal("geometry not valid", g.AsWKT(rel.Geom.Geom))
	}

	if area := rel.Geom.Geom.Area(); area != 100 {
		t.Fatal("area invalid", area)
	}
}
