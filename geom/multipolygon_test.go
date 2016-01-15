package geom

import (
	"math"
	"testing"

	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/geom/geos"
)

type coord struct {
	id   int64
	long float64
	lat  float64
}

func makeWay(id int64, tags element.Tags, coords []coord) element.Way {
	way := element.Way{}
	way.Id = id
	way.Tags = tags
	for _, coord := range coords {
		way.Refs = append(way.Refs, coord.id)
		way.Nodes = append(way.Nodes,
			element.Node{OSMElem: element.OSMElem{Id: coord.id}, Long: coord.long, Lat: coord.lat})
	}
	return way
}

func buildRelation(rel *element.Relation, srid int) (Geometry, error) {
	prep, err := PrepareRelation(rel, srid, 0.1)
	if err != nil {
		return Geometry{}, err
	}
	return prep.Build()
}

func TestSimplePolygonWithHole(t *testing.T) {
	w1 := makeWay(1, element.Tags{}, []coord{
		{1, 0, 0},
		{2, 10, 0},
		{3, 10, 10},
		{4, 0, 10},
		{1, 0, 0},
	})
	w2 := makeWay(2, element.Tags{}, []coord{
		{5, 2, 2},
		{6, 8, 2},
		{7, 8, 8},
		{8, 2, 8},
		{5, 2, 2},
	})

	rel := element.Relation{
		OSMElem: element.OSMElem{Id: 1, Tags: element.Tags{}}}
	rel.Members = []element.Member{
		{Id: 1, Type: element.WAY, Role: "outer", Way: &w1},
		{Id: 2, Type: element.WAY, Role: "inner", Way: &w2},
	}

	geom, err := buildRelation(&rel, 3857)
	if err != nil {
		t.Fatal(err)
	}
	g := geos.NewGeos()
	defer g.Finish()

	if len(rel.Tags) != 0 {
		t.Fatal("wrong rel tags", rel.Tags)
	}

	if !g.IsValid(geom.Geom) {
		t.Fatal("geometry not valid", g.AsWkt(geom.Geom))
	}

	if area := geom.Geom.Area(); area != 100-36 {
		t.Fatal("area invalid", area)
	}
}

func TestMultiPolygonWithHoleAndRelName(t *testing.T) {
	w1 := makeWay(1, element.Tags{"natural": "forest", "name": "Blackwood"}, []coord{
		{1, 0, 0},
		{2, 10, 0},
		{3, 10, 10},
		{4, 0, 10},
		{1, 0, 0},
	})
	w2 := makeWay(1, element.Tags{"landusage": "scrub"}, []coord{
		{5, 2, 2},
		{6, 8, 2},
		{7, 8, 8},
		{8, 2, 8},
		{5, 2, 2},
	})

	rel := element.Relation{
		OSMElem: element.OSMElem{Id: 1, Tags: element.Tags{"name": "rel"}}}
	rel.Members = []element.Member{
		{Id: 1, Type: element.WAY, Role: "outer", Way: &w1},
		{Id: 2, Type: element.WAY, Role: "inner", Way: &w2},
	}

	geom, err := buildRelation(&rel, 3857)
	if err != nil {
		t.Fatal(err)
	}
	g := geos.NewGeos()
	defer g.Finish()

	if len(rel.Tags) != 2 {
		t.Fatal("wrong rel tags", rel.Tags)
	}
	if rel.Tags["natural"] != "forest" || rel.Tags["name"] != "Blackwood" {
		t.Fatal("wrong rel tags", rel.Tags)
	}

	if !g.IsValid(geom.Geom) {
		t.Fatal("geometry not valid", g.AsWkt(geom.Geom))
	}

	if area := geom.Geom.Area(); area != 64 {
		t.Fatal("aread not 64", area)
	}
}

func TestMultiPolygonWithMultipleHoles(t *testing.T) {
	w1 := makeWay(1, element.Tags{"landusage": "forest"}, []coord{
		{1, 0, 0},
		{2, 10, 0},
		{3, 10, 10},
		{4, 0, 10},
		{1, 0, 0},
	})
	w2 := makeWay(1, element.Tags{"water": "basin"}, []coord{
		{1, 1, 1},
		{2, 2, 1},
		{3, 2, 2},
		{4, 1, 2},
		{1, 1, 1},
	})
	w3 := makeWay(3, element.Tags{"landusage": "scrub"}, []coord{
		{1, 3, 3},
		{2, 4, 3},
		{3, 4, 4},
		{4, 3, 4},
		{1, 3, 3},
	})

	rel := element.Relation{
		OSMElem: element.OSMElem{Id: 1, Tags: element.Tags{"landusage": "forest"}}}
	rel.Members = []element.Member{
		{Id: 1, Type: element.WAY, Role: "outer", Way: &w1},
		{Id: 2, Type: element.WAY, Role: "inner", Way: &w2},
		{Id: 3, Type: element.WAY, Role: "inner", Way: &w3},
	}

	geom, err := buildRelation(&rel, 3857)
	if err != nil {
		t.Fatal(err)
	}
	g := geos.NewGeos()
	defer g.Finish()

	if len(rel.Tags) != 1 {
		t.Fatal("wrong rel tags", rel.Tags)
	}
	if rel.Tags["landusage"] != "forest" {
		t.Fatal("wrong rel tags", rel.Tags)
	}

	if !g.IsValid(geom.Geom) {
		t.Fatal("geometry not valid", g.AsWkt(geom.Geom))
	}

	if area := geom.Geom.Area(); area != 100-1-1 {
		t.Fatal("area invalid", area)
	}
}

func TestMultiPolygonWithNeastedHoles(t *testing.T) {
	w1 := makeWay(1, element.Tags{"landusage": "forest"}, []coord{
		{1, 0, 0},
		{2, 10, 0},
		{3, 10, 10},
		{4, 0, 10},
		{1, 0, 0},
	})
	w2 := makeWay(2, element.Tags{"landusage": "scrub"}, []coord{
		{1, 1, 1},
		{2, 9, 1},
		{3, 9, 9},
		{4, 1, 9},
		{1, 1, 1},
	})
	w3 := makeWay(3, element.Tags{}, []coord{
		{1, 2, 2},
		{2, 8, 2},
		{3, 8, 8},
		{4, 2, 8},
		{1, 2, 2},
	})
	w4 := makeWay(4, element.Tags{"landusage": "scrub"}, []coord{
		{1, 3, 3},
		{2, 7, 3},
		{3, 7, 7},
		{4, 3, 7},
		{1, 3, 3},
	})
	w5 := makeWay(5, element.Tags{"landusage": "forest"}, []coord{
		{1, 4, 4},
		{2, 6, 4},
		{3, 6, 6},
		{4, 4, 6},
		{1, 4, 4},
	})

	rel := element.Relation{OSMElem: element.OSMElem{Id: 1}}
	rel.Members = []element.Member{
		{Id: 1, Type: element.WAY, Role: "outer", Way: &w1},
		{Id: 2, Type: element.WAY, Role: "inner", Way: &w2},
		{Id: 3, Type: element.WAY, Role: "inner", Way: &w3},
		{Id: 4, Type: element.WAY, Role: "inner", Way: &w4},
		{Id: 5, Type: element.WAY, Role: "inner", Way: &w5},
	}

	geom, err := buildRelation(&rel, 3857)
	if err != nil {
		t.Fatal(err)
	}
	g := geos.NewGeos()
	defer g.Finish()

	if len(rel.Tags) != 1 {
		t.Fatal("wrong rel tags", rel.Tags)
	}
	if rel.Tags["landusage"] != "forest" {
		t.Fatal("wrong rel tags", rel.Tags)
	}

	if !g.IsValid(geom.Geom) {
		t.Fatal("geometry not valid", g.AsWkt(geom.Geom))
	}

	if area := geom.Geom.Area(); area != 100-64+36-16+4 {
		t.Fatal("area invalid", area)
	}
}

func TestPolygonFromThreeWays(t *testing.T) {
	w1 := makeWay(1, element.Tags{"landusage": "forest"}, []coord{
		{1, 0, 0},
		{2, 10, 0},
		{3, 10, 10},
	})
	w2 := makeWay(2, element.Tags{"landusage": "water"}, []coord{
		{3, 10, 10},
		{4, 0, 10},
	})
	w3 := makeWay(3, element.Tags{"landusage": "forest"}, []coord{
		{4, 0, 10},
		{1, 0, 0},
	})

	rel := element.Relation{OSMElem: element.OSMElem{Id: 1}}
	rel.Members = []element.Member{
		{Id: 1, Type: element.WAY, Role: "outer", Way: &w1},
		{Id: 2, Type: element.WAY, Role: "inner", Way: &w2},
		{Id: 3, Type: element.WAY, Role: "inner", Way: &w3},
	}

	geom, err := buildRelation(&rel, 3857)
	if err != nil {
		t.Fatal(err)
	}
	g := geos.NewGeos()
	defer g.Finish()

	if len(rel.Tags) != 1 {
		t.Fatal("wrong rel tags", rel.Tags)
	}
	if rel.Tags["landusage"] != "forest" {
		t.Fatal("wrong rel tags", rel.Tags)
	}

	if !g.IsValid(geom.Geom) {
		t.Fatal("geometry not valid", g.AsWkt(geom.Geom))
	}

	if area := geom.Geom.Area(); area != 100 {
		t.Fatal("area invalid", area)
	}
}

func TestTouchingPolygonsWithHole(t *testing.T) {
	w1 := makeWay(1, element.Tags{"water": "riverbank"}, []coord{
		{1, 0, 0},
		{2, 10, 0},
		{3, 10, 10},
		{4, 0, 10},
		{1, 0, 0},
	})
	w2 := makeWay(2, element.Tags{"water": "riverbank"}, []coord{
		{2, 10, 0},
		{5, 30, 0},
		{6, 30, 10},
		{3, 10, 10},
		{2, 10, 0},
	})
	w3 := makeWay(3, element.Tags{"landusage": "forest"}, []coord{
		{7, 2, 2},
		{8, 8, 2},
		{9, 8, 8},
		{10, 2, 8},
		{7, 2, 2},
	})

	rel := element.Relation{OSMElem: element.OSMElem{Id: 1, Tags: element.Tags{"water": "riverbank"}}}
	rel.Members = []element.Member{
		{Id: 1, Type: element.WAY, Role: "outer", Way: &w1},
		{Id: 2, Type: element.WAY, Role: "outer", Way: &w2},
		{Id: 3, Type: element.WAY, Role: "inner", Way: &w3},
	}
	geom, err := buildRelation(&rel, 3857)
	if err != nil {
		t.Fatal(err)
	}
	g := geos.NewGeos()
	defer g.Finish()

	if len(rel.Tags) != 1 {
		t.Fatal("wrong rel tags", rel.Tags)
	}

	if rel.Tags["water"] != "riverbank" {
		t.Fatal("wrong rel tags", rel.Tags)
	}

	if !g.IsValid(geom.Geom) {
		t.Fatal("geometry not valid", g.AsWkt(geom.Geom))
	}

	if area := geom.Geom.Area(); area != 100+200-36 {
		t.Fatal("area invalid", area)
	}
}

func TestInsertedWaysDifferentTags(t *testing.T) {
	w1 := makeWay(1, element.Tags{"landusage": "forest"}, []coord{
		{1, 0, 0},
		{2, 10, 0},
		{3, 10, 10},
	})
	w2 := makeWay(2, element.Tags{"highway": "secondary"}, []coord{
		{3, 10, 10},
		{4, 0, 10},
		{1, 0, 0},
	})

	rel := element.Relation{OSMElem: element.OSMElem{Id: 1, Tags: element.Tags{"landusage": "forest"}}}
	rel.Members = []element.Member{
		{Id: 1, Type: element.WAY, Role: "outer", Way: &w1},
		{Id: 2, Type: element.WAY, Role: "inner", Way: &w2},
	}

	geom, err := buildRelation(&rel, 3857)
	if err != nil {
		t.Fatal(err)
	}
	g := geos.NewGeos()
	defer g.Finish()

	if len(rel.Tags) != 1 {
		t.Fatal("wrong rel tags", rel.Tags)
	}

	if rel.Tags["landusage"] != "forest" {
		t.Fatal("wrong rel tags", rel.Tags)
	}

	if !g.IsValid(geom.Geom) {
		t.Fatal("geometry not valid", g.AsWkt(geom.Geom))
	}

	if area := geom.Geom.Area(); area != 100 {
		t.Fatal("area invalid", area)
	}
}

func TestInsertMultipleTags(t *testing.T) {
	w1 := makeWay(1, element.Tags{"landusage": "forest", "highway": "secondary"}, []coord{
		{1, 0, 0},
		{2, 10, 0},
		{3, 10, 10},
	})
	w2 := makeWay(2, element.Tags{"highway": "secondary"}, []coord{
		{3, 10, 10},
		{4, 0, 10},
		{1, 0, 0},
	})

	rel := element.Relation{OSMElem: element.OSMElem{Id: 1, Tags: element.Tags{"landusage": "forest"}}}
	rel.Members = []element.Member{
		{Id: 1, Type: element.WAY, Role: "outer", Way: &w1}, // also highway=secondary
		{Id: 2, Type: element.WAY, Role: "inner", Way: &w2},
	}

	geom, err := buildRelation(&rel, 3857)
	if err != nil {
		t.Fatal(err)
	}
	g := geos.NewGeos()
	defer g.Finish()

	if rel.Tags["landusage"] != "forest" {
		t.Fatal("wrong rel tags", rel.Tags)
	}

	if !g.IsValid(geom.Geom) {
		t.Fatal("geometry not valid", g.AsWkt(geom.Geom))
	}

	if area := geom.Geom.Area(); area != 100 {
		t.Fatal("area invalid", area)
	}
}

func TestBrokenPolygonSelfIntersect(t *testing.T) {
	//  2##3    6##7
	//  #  #    ####
	//  1##4____5##8
	w1 := makeWay(1, element.Tags{}, []coord{
		{1, 0, 0},
		{2, 0, 10},
		{3, 10, 10},
		{4, 10, 0},
		{5, 20, 0},
		{6, 20, 10},
		{7, 30, 10},
		{8, 30, 0},
		{1, 0, 0},
	})
	w2 := makeWay(2, element.Tags{}, []coord{
		{15, 2, 2},
		{16, 8, 2},
		{17, 8, 8},
		{18, 2, 8},
		{15, 2, 2},
	})

	rel1 := element.Relation{OSMElem: element.OSMElem{Id: 1}}
	rel1.Members = []element.Member{
		{Id: 1, Type: element.WAY, Role: "outer", Way: &w1},
		{Id: 2, Type: element.WAY, Role: "inner", Way: &w2},
	}

	geom1, err := buildRelation(&rel1, 3857)
	if err != nil {
		t.Fatal(err)
	}
	g := geos.NewGeos()
	defer g.Finish()

	if len(rel1.Tags) != 0 {
		t.Fatal("wrong rel tags", rel1.Tags)
	}

	if !g.IsValid(geom1.Geom) {
		t.Fatal("geometry not valid", g.AsWkt(geom1.Geom))
	}

	if area := geom1.Geom.Area(); area != 200-36 {
		t.Fatal("area invalid", area)
	}

	//  2##3    6##7
	//  #  #    ####
	//  1##4____5##8
	w3 := makeWay(1, element.Tags{}, []coord{
		{4, 10, 0},
		{1, 0, 0},
		{2, 0, 10},
		{3, 10, 10},
		{4, 10, 0},
		{5, 20, 0},
		{6, 20, 10},
		{7, 30, 10},
		{8, 30, 0},
		{4, 10, 0},
	})

	rel2 := element.Relation{OSMElem: element.OSMElem{Id: 1}}
	rel2.Members = []element.Member{
		{Id: 1, Type: element.WAY, Role: "outer", Way: &w3},
		{Id: 2, Type: element.WAY, Role: "inner", Way: &w2},
	}

	geom2, err := buildRelation(&rel2, 3857)
	if err != nil {
		t.Fatal(err)
	}

	g = geos.NewGeos()
	defer g.Finish()

	if len(rel2.Tags) != 0 {
		t.Fatal("wrong rel tags", rel2.Tags)
	}

	if !g.IsValid(geom2.Geom) {
		t.Fatal("geometry not valid", g.AsWkt(geom2.Geom))
	}

	if area := geom2.Geom.Area(); area != 200-36 {
		t.Fatal("area invalid", area)
	}
}

func TestBrokenPolygonSelfIntersectTriangle(t *testing.T) {
	// 2###
	// #    ###4
	// #    ###3
	// 1###
	// triangle with four points, minor overlapping

	w1 := makeWay(1, element.Tags{}, []coord{
		{1, 0, 0},
		{2, 0, 100},
		{3, 100, 50 - 0.00001},
		{4, 100, 50 + 0.00001},
		{1, 0, 0},
	})
	w2 := makeWay(2, element.Tags{}, []coord{
		{15, 10, 45},
		{16, 10, 55},
		{17, 20, 55},
		{18, 20, 45},
		{15, 10, 45},
	})

	rel := element.Relation{OSMElem: element.OSMElem{Id: 1}}
	rel.Members = []element.Member{
		{Id: 1, Type: element.WAY, Role: "outer", Way: &w1},
		{Id: 2, Type: element.WAY, Role: "inner", Way: &w2},
	}

	geom, err := buildRelation(&rel, 3857)
	if err != nil {
		t.Fatal(err)
	}

	g := geos.NewGeos()
	defer g.Finish()

	if !g.IsValid(geom.Geom) {
		t.Fatal("geometry not valid", g.AsWkt(geom.Geom))
	}

	area := geom.Geom.Area()
	// as for python assertAlmostEqual(a, b)	round(a-b, 7) == 0
	if math.Abs(area-(100*100/2-100)) > 0.01 {
		t.Fatal("area invalid", area)
	}

	// larger overlap
	w3 := makeWay(1, element.Tags{}, []coord{
		{1, 0, 0},
		{2, 0, 100},
		{3, 100, 50 - 1},
		{4, 100, 50 + 1},
		{1, 0, 0},
	})
	w4 := makeWay(2, element.Tags{}, []coord{
		{15, 10, 45},
		{16, 10, 55},
		{17, 20, 55},
		{18, 20, 45},
		{15, 10, 45},
	})

	rel = element.Relation{OSMElem: element.OSMElem{Id: 1}}
	rel.Members = []element.Member{
		{Id: 1, Type: element.WAY, Role: "outer", Way: &w3},
		{Id: 2, Type: element.WAY, Role: "inner", Way: &w4},
	}

	geom, err = buildRelation(&rel, 3857)
	if err != nil {
		t.Fatal(err)
	}

	if !g.IsValid(geom.Geom) {
		t.Fatal("geometry not valid", g.AsWkt(geom.Geom))
	}

	area = geom.Geom.Area()
	if math.Abs((area - (100*98/2 - 100))) > 10 {
		t.Fatal("area invalid", area)

	}
}

func TestOpenRing(t *testing.T) {
	w1 := makeWay(1, element.Tags{}, []coord{
		{1, 0, 0},
		{2, 10, 0},
		{3, 10, 10},
		{4, 0, 10},
	})

	rel := element.Relation{
		OSMElem: element.OSMElem{Id: 1, Tags: element.Tags{}}}
	rel.Members = []element.Member{
		{Id: 1, Type: element.WAY, Role: "outer", Way: &w1},
	}

	_, err := buildRelation(&rel, 3857)
	if err == nil {
		t.Fatal("no error from open ring")
	}
}

func TestClosedAndOpenRing(t *testing.T) {
	w1 := makeWay(1, element.Tags{}, []coord{
		{1, 0, 0},
		{2, 10, 0},
		{3, 10, 10},
		{4, 0, 10},
		{1, 0, 0},
	})
	w2 := makeWay(2, element.Tags{}, []coord{
		{5, 0, 0},
		{6, -5, -2},
	})
	rel := element.Relation{
		OSMElem: element.OSMElem{Id: 1, Tags: element.Tags{}}}
	rel.Members = []element.Member{
		{Id: 1, Type: element.WAY, Role: "outer", Way: &w1},
		{Id: 2, Type: element.WAY, Role: "outer", Way: &w2},
	}

	prep, err := PrepareRelation(&rel, 3857, 0.1)
	if err != nil {
		t.Fatal(err)
	}
	// open ring is excluded
	if len(prep.rings) != 1 {
		t.Fatal("expected single ring")
	}
	geom, err := prep.Build()
	if err != nil {
		t.Fatal(err)
	}

	g := geos.NewGeos()
	defer g.Finish()

	if !g.IsValid(geom.Geom) {
		t.Fatal("geometry not valid", g.AsWkt(geom.Geom))
	}
}
