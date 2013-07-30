package clipper

import (
	"goposm/geom/geos"
	"testing"
)

func TestTileBounds(t *testing.T) {
	expected := []geos.Bounds{
		{-1.0, 1.0, -0.5, 1.5},
		{-1.0, 1.5, -0.5, 2.0},
		{-0.5, 1.0, 0.0, 1.5},
		{-0.5, 1.5, 0.0, 2.0},
		{0.0, 1.0, 0.5, 1.5},
		{0.0, 1.5, 0.5, 2.0},
	}
	for i, bounds := range tileBounds(geos.Bounds{-1, 1, 0.49, 1.51}, 0.5) {
		if expected[i] != bounds {
			t.Fatalf("%v != %v\n", expected[i], bounds)
		}
	}
}

func TestSplitPolygonAtGrids(t *testing.T) {
	expected := []geos.Bounds{
		{0, 0, 0.05, 0.05},
		{0, 0.05, 0.05, 0.1},
		{0, 0.1, 0.05, 0.11},
		{0.05, 0, 0.1, 0.05},
		{0.05, 0.05, 0.1, 0.1},
		{0.05, 0.1, 0.1, 0.11},
		{0.1, 0, 0.15, 0.05},
		{0.1, 0.05, 0.15, 0.1},
		{0.1, 0.1, 0.15, 0.11},
	}

	g := geos.NewGeos()
	defer g.Finish()

	geom := g.BoundsPolygon(geos.Bounds{0, 0, 0.15, 0.11})

	geoms, _ := SplitPolygonAtGrid(g, geom, 0.05, 5.0)
	for i, geom := range geoms {
		if expected[i] != geom.Bounds() {
			t.Fatalf("%v != %v\n", expected[i], geom.Bounds())
		}
	}

}

func TestMergePolygonGeometries(t *testing.T) {
	g := geos.NewGeos()
	defer g.Finish()

	// check non intersecting polygons
	// should return multipolygon
	geoms := []*geos.Geom{
		g.BoundsPolygon(geos.Bounds{0, 0, 10, 10}),
		g.BoundsPolygon(geos.Bounds{20, 20, 30, 30}),
	}
	result := mergeGeometries(g, geoms, "Polygon")

	if len(result) != 1 {
		t.Fatal("not a single geometrie")
	}
	if g.Type(result[0]) != "MultiPolygon" {
		t.Fatal("not a multipolygon")
	}
	if !g.IsValid(result[0]) {
		t.Fatal("not valid")
	}

	// check intersecting polygons
	// should return single polygon
	geoms = []*geos.Geom{
		g.BoundsPolygon(geos.Bounds{0, 0, 10, 10}),
		g.BoundsPolygon(geos.Bounds{5, 5, 30, 30}),
	}
	result = mergeGeometries(g, geoms, "Polygon")

	if len(result) != 1 {
		t.Fatal("not a single geometrie")
	}
	if g.Type(result[0]) != "Polygon" {
		t.Fatal("not a polygon")
	}
	if !g.IsValid(result[0]) {
		t.Fatal("not valid")
	}

	// same with multipolygon type
	geoms = []*geos.Geom{
		g.BoundsPolygon(geos.Bounds{0, 0, 10, 10}),
		g.BoundsPolygon(geos.Bounds{5, 5, 30, 30}),
	}
	result = mergeGeometries(g, geoms, "MultiPolygon")

	if len(result) != 1 {
		t.Fatal("not a single geometrie")
	}
	if g.Type(result[0]) != "Polygon" {
		t.Fatal("not a polygon")
	}
	if !g.IsValid(result[0]) {
		t.Fatal("not valid")
	}

	// strip non Polygons
	geoms = []*geos.Geom{
		g.FromWkt("POINT(0 0)"),
		g.BoundsPolygon(geos.Bounds{0, 0, 10, 10}),
		g.FromWkt("LINESTRING(0 0, 0 10)"),
		g.BoundsPolygon(geos.Bounds{5, 5, 30, 30}),
	}
	result = mergeGeometries(g, geoms, "Polygon")

	if len(result) != 1 {
		t.Fatal("not a single geometrie")
	}
	if g.Type(result[0]) != "Polygon" {
		t.Fatal("not a polygon")
	}
	if !g.IsValid(result[0]) {
		t.Fatal("not valid")
	}
}

func TestMergeLineStringGeometries(t *testing.T) {
	g := geos.NewGeos()
	defer g.Finish()

	// check non intersecting linestrings
	// should return slice of two linestrings
	geoms := []*geos.Geom{
		g.FromWkt("LINESTRING(0 0, 10 0)"),
		g.FromWkt("LINESTRING(20 0, 30 0)"),
	}
	result := mergeGeometries(g, geoms, "LineString")

	if len(result) != 2 {
		t.Fatal("not two lines")
	}
	if g.Type(result[0]) != "LineString" || g.Type(result[1]) != "LineString" {
		t.Fatal("not two lines")
	}
	if !g.IsValid(result[0]) || !g.IsValid(result[1]) {
		t.Fatal("not valid")
	}

	// check intersecting linestrings
	// should return slice of a single merged linestring
	geoms = []*geos.Geom{
		g.FromWkt("LINESTRING(0 0, 10 0)"),
		g.FromWkt("LINESTRING(0 0, 0 10)"),
		g.FromWkt("LINESTRING(10 0, 10 10)"),
	}
	result = mergeGeometries(g, geoms, "LineString")

	if len(result) != 1 {
		t.Fatal("not a single geometrie")
	}
	if g.Type(result[0]) != "LineString" {
		t.Fatal("not a linestring")
	}
	if !g.IsValid(result[0]) {
		t.Fatal("not valid")
	}
	if result[0].Length() != 30 {
		t.Fatal("wrong length", result[0].Length())
	}

	// same but with multilinestring type
	geoms = []*geos.Geom{
		g.FromWkt("LINESTRING(0 0, 10 0)"),
		g.FromWkt("MULTILINESTRING((0 0, 0 10), (10 0, 10 10))"),
	}
	result = mergeGeometries(g, geoms, "MultiLineString")

	if len(result) != 1 {
		t.Fatal("not a single geometrie")
	}
	if g.Type(result[0]) != "LineString" {
		t.Fatal("not a linestring")
	}
	if !g.IsValid(result[0]) {
		t.Fatal("not valid")
	}
	if result[0].Length() != 30 {
		t.Fatal("wrong length", result[0].Length())
	}

	// strip non LineStrings and tiny LineStrings
	geoms = []*geos.Geom{
		g.FromWkt("POINT(0 0)"),
		g.FromWkt("LINESTRING(0 0, 0 10)"),
		g.FromWkt("LINESTRING(20 20, 20.00000000001 20)"), // tiny length
		g.FromWkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"),
	}
	result = mergeGeometries(g, geoms, "LineString")

	if len(result) != 1 {
		t.Fatal("not a single geometrie")
	}
	if g.Type(result[0]) != "LineString" {
		t.Fatal("not a linestring")
	}
	if !g.IsValid(result[0]) {
		t.Fatal("not valid")
	}
}

func TestFilterGeometryByType(t *testing.T) {
	g := geos.NewGeos()
	defer g.Finish()

	var result []*geos.Geom

	// filtered out
	result = filterGeometryByType(g, g.FromWkt("POINT(0 0)"), "Polygon")
	if len(result) != 0 {
		t.Fatal()
	}
	result = filterGeometryByType(g, g.FromWkt("POINT(0 0)"), "Point")
	if len(result) != 1 {
		t.Fatal()
	}

	// filtered out
	result = filterGeometryByType(g, g.FromWkt("LINESTRING(0 0, 10 0)"), "Polygon")
	if len(result) != 0 {
		t.Fatal()
	}

	// polygon <-> multipolygon types are compatible in both directions
	result = filterGeometryByType(g, g.FromWkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"), "Polygon")
	if len(result) != 1 {
		t.Fatal()
	}
	result = filterGeometryByType(g, g.FromWkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"), "MultiPolygon")
	if len(result) != 1 {
		t.Fatal()
	}
	result = filterGeometryByType(g, g.FromWkt("MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)))"), "Polygon")
	if len(result) != 1 {
		t.Fatal()
	}

	result = filterGeometryByType(g, g.FromWkt("LINESTRING(0 0, 10 0)"), "LineString")
	if len(result) != 1 {
		t.Fatal()
	}
	// multilinestrings are split
	result = filterGeometryByType(g, g.FromWkt("MULTILINESTRING((0 0, 10 0), (20 0, 30 0))"), "LineString")
	if len(result) != 2 {
		t.Fatal()
	}

}

func TestClipper(t *testing.T) {
	g := geos.NewGeos()
	defer g.Finish()
	clipper, err := NewFromOgrSource("./hamburg_clip.geojson")
	if err != nil {
		t.Fatal(err)
	}

	result, err := clipper.Clip(g.FromWkt("POINT(0 0)"))
	if err != nil || result != nil {
		t.Fatal(err)
	}

	result, err = clipper.Clip(g.FromWkt("POINT(1106543 7082055)"))
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatal()
	}

	result, err = clipper.Clip(g.FromWkt("LINESTRING(1106543 7082055, 1107105.2 7087540.0)"))
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Fatal()
	}

	geom := g.FromWkt("POLYGON((1106543 7082055, 1107105.2 7087540.0, 1112184.9 7084424.5, 1106543 7082055))")
	result, err = clipper.Clip(geom)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatal()
	}
	if geom.Area() <= result[0].Area() {
		t.Fatalf("%f <= %f", geom.Area(), result[0].Area())
	}
}

func TestClipperWithBuffer(t *testing.T) {
	g := geos.NewGeos()
	defer g.Finish()
	clipper, err := NewFromOgrSourceWithBuffered("./hamburg_clip.geojson", 10000.0)
	if err != nil {
		t.Fatal(err)
	}
	if clipper.IntersectsBuffer(g, 1106543, 7082055) != true {
		t.Fatal()
	}
	if clipper.IntersectsBuffer(g, 1006543, 7082055) != false {
		t.Fatal()
	}
}

func BenchmarkClipper(b *testing.B) {
	g := geos.NewGeos()
	defer g.Finish()
	clipper, err := NewFromOgrSource("./hamburg_clip.geojson")
	if err != nil {
		b.Fatal(err)
	}

	geom := g.FromWkt("LINESTRING(1106543 7082055, 1107105.2 7087540.0)")
	for i := 0; i < b.N; i++ {
		result, err := clipper.Clip(geom)
		if err != nil {
			b.Fatal(err)
		}
		if len(result) != 2 {
			b.Fatal()
		}
	}
}
