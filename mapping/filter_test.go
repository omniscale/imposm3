package mapping

import (
	"imposm3/element"
	"testing"
)

var mapping *Mapping

func init() {
	var err error
	mapping, err = NewMapping("./test_mapping.json")
	if err != nil {
		panic(err)
	}
}

func stringMapEquals(t *testing.T, expected, actual map[string]string) {
	if len(expected) != len(actual) {
		t.Fatalf("different length in %v and %v\n", expected, actual)
	}

	for k, v := range expected {
		if actualV, ok := actual[k]; ok {
			if actualV != v {
				t.Fatalf("%s != %s in %v and %v\n", v, actualV, expected, actual)
			}
		} else {
			t.Fatalf("%s not in %v\n", k, actual)
		}
	}
}

func matchesEqual(t *testing.T, expected []Match, actual []Match) {
	expectedMatches := make(map[DestTable]Match)
	actualMatches := make(map[DestTable]Match)

	if len(expected) != len(actual) {
		t.Fatalf("different length in %v and %v\n", expected, actual)
	}

	for _, match := range expected {
		expectedMatches[match.Table] = match
	}
	for _, match := range actual {
		actualMatches[match.Table] = match
	}

	for name, expectedMatch := range expectedMatches {
		if actualMatch, ok := actualMatches[name]; ok {
			if expectedMatch.Table != actualMatch.Table ||
				expectedMatch.Key != actualMatch.Key ||
				expectedMatch.Value != actualMatch.Value {
				t.Fatalf("match differ %v != %v", expectedMatch, actualMatch)
			}
		} else {
			t.Fatalf("%s not in %v", name, actualMatches)
		}
	}
}

func TestTagFilterNodes(t *testing.T) {
	var tags element.Tags
	nodes := mapping.NodeTagFilter()

	tags = element.Tags{"name": "foo"}
	if nodes.Filter(&tags) != false {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{}, tags)

	tags = element.Tags{"name": "foo", "unknown": "baz"}
	if nodes.Filter(&tags) != false {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{}, tags)

	tags = element.Tags{"name": "foo", "place": "unknown"}
	if nodes.Filter(&tags) != false {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{}, tags)

	tags = element.Tags{"name": "foo", "place": "village"}
	if nodes.Filter(&tags) != true {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{"name": "foo", "place": "village"}, tags)

	tags = element.Tags{"name": "foo", "place": "village", "population": "1000"}
	if nodes.Filter(&tags) != true {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{"name": "foo", "place": "village", "population": "1000"}, tags)

	tags = element.Tags{"name": "foo", "place": "village", "highway": "unknown"}
	if nodes.Filter(&tags) != true {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{"name": "foo", "place": "village"}, tags)

	tags = element.Tags{"name": "foo", "place": "village", "highway": "bus_stop"}
	if nodes.Filter(&tags) != true {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{"name": "foo", "place": "village", "highway": "bus_stop"}, tags)
}

func TestTagFilterWays(t *testing.T) {
	var tags element.Tags
	ways := mapping.WayTagFilter()

	tags = element.Tags{"name": "foo"}
	if ways.Filter(&tags) != false {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{}, tags)

	tags = element.Tags{"name": "foo", "unknown": "baz"}
	if ways.Filter(&tags) != false {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{}, tags)

	tags = element.Tags{"name": "foo", "highway": "unknown"}
	if ways.Filter(&tags) != false {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{}, tags)

	tags = element.Tags{"name": "foo", "highway": "track"}
	if ways.Filter(&tags) != true {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{"name": "foo", "highway": "track"}, tags)

	tags = element.Tags{"name": "foo", "highway": "track", "oneway": "yes", "tunnel": "1"}
	if ways.Filter(&tags) != true {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{"name": "foo", "highway": "track", "oneway": "yes", "tunnel": "1"}, tags)

	tags = element.Tags{"name": "foo", "place": "village", "highway": "track"}
	if ways.Filter(&tags) != true {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{"name": "foo", "highway": "track"}, tags)

	tags = element.Tags{"name": "foo", "railway": "tram", "highway": "secondary"}
	if ways.Filter(&tags) != true {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{"name": "foo", "railway": "tram", "highway": "secondary"}, tags)

	// with __any__ value
	tags = element.Tags{"name": "foo", "building": "yes"}
	if ways.Filter(&tags) != true {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{"name": "foo", "building": "yes"}, tags)

	tags = element.Tags{"name": "foo", "building": "whatever"}
	if ways.Filter(&tags) != true {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{"name": "foo", "building": "whatever"}, tags)
}

func TestTagFilterRelations(t *testing.T) {
	var tags element.Tags
	relations := mapping.RelationTagFilter()

	tags = element.Tags{"name": "foo"}
	if relations.Filter(&tags) != false {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{}, tags)

	tags = element.Tags{"name": "foo", "unknown": "baz"}
	if relations.Filter(&tags) != false {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{}, tags)

	tags = element.Tags{"name": "foo", "landuse": "unknown"}
	if relations.Filter(&tags) != false {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{}, tags)

	tags = element.Tags{"name": "foo", "landuse": "farm"}
	if relations.Filter(&tags) != false {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{}, tags)

	tags = element.Tags{"name": "foo", "landuse": "farm", "type": "multipolygon"}
	if relations.Filter(&tags) != true {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{"name": "foo", "landuse": "farm", "type": "multipolygon"}, tags)

	// skip multipolygon with filtered tags, otherwise tags from
	// longest way would be used
	tags = element.Tags{"name": "foo", "landuse": "unknown", "type": "multipolygon"}
	if relations.Filter(&tags) != false {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{}, tags)

	tags = element.Tags{"name": "foo", "landuse": "park", "type": "multipolygon"}
	if relations.Filter(&tags) != true {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{"name": "foo", "type": "multipolygon", "landuse": "park"}, tags)

	tags = element.Tags{"name": "foo", "landuse": "farm", "boundary": "administrative", "type": "multipolygon"}
	if relations.Filter(&tags) != true {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{"name": "foo", "landuse": "farm", "boundary": "administrative", "type": "multipolygon"}, tags)

	// boundary relation for boundary
	tags = element.Tags{"name": "foo", "landuse": "farm", "boundary": "administrative", "type": "boundary"}
	if relations.Filter(&tags) != true {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{"name": "foo", "landuse": "farm", "boundary": "administrative", "type": "boundary"}, tags)

	// boundary relation for non boundary
	tags = element.Tags{"name": "foo", "landuse": "farm", "type": "boundary"}
	if relations.Filter(&tags) != false {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{}, tags)

	/* skip boundary with filtered tags, otherwise tags from longest way would
	be used */
	tags = element.Tags{"name": "foo", "boundary": "unknown", "type": "boundary"}
	if relations.Filter(&tags) != false {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{}, tags)

	tags = element.Tags{"name": "foo", "boundary": "administrative", "type": "boundary"}
	if relations.Filter(&tags) != true {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{"name": "foo", "boundary": "administrative", "type": "boundary"}, tags)

}

func TestPointMatcher(t *testing.T) {
	var tags element.Tags
	points := mapping.PointMatcher()

	tags = element.Tags{"unknown": "baz"}
	matchesEqual(t, []Match{}, points.Match(&tags))

	tags = element.Tags{"place": "unknown"}
	matchesEqual(t, []Match{}, points.Match(&tags))

	tags = element.Tags{"place": "city"}
	matchesEqual(t, []Match{{"place", "city", DestTable{"places", ""}, nil}}, points.Match(&tags))

	tags = element.Tags{"place": "city", "highway": "unknown"}
	matchesEqual(t, []Match{{"place", "city", DestTable{"places", ""}, nil}}, points.Match(&tags))

	tags = element.Tags{"place": "city", "highway": "bus_stop"}
	matchesEqual(t,
		[]Match{
			{"place", "city", DestTable{"places", ""}, nil},
			{"highway", "bus_stop", DestTable{"transport_points", ""}, nil}},
		points.Match(&tags))
}

func TestLineStringMatcher(t *testing.T) {
	var tags element.Tags
	ls := mapping.LineStringMatcher()

	tags = element.Tags{"unknown": "baz"}
	matchesEqual(t, []Match{}, ls.Match(&tags))

	tags = element.Tags{"highway": "unknown"}
	matchesEqual(t, []Match{}, ls.Match(&tags))

	tags = element.Tags{"highway": "pedestrian"}
	matchesEqual(t, []Match{{"highway", "pedestrian", DestTable{"roads", "roads"}, nil}}, ls.Match(&tags))

	// exclude_tags area=yes
	tags = element.Tags{"highway": "pedestrian", "area": "yes"}
	matchesEqual(t, []Match{}, ls.Match(&tags))

	tags = element.Tags{"highway": "secondary", "railway": "tram"}
	matchesEqual(t,
		[]Match{
			{"highway", "secondary", DestTable{"roads", "roads"}, nil},
			{"railway", "tram", DestTable{"roads", "railway"}, nil}},
		ls.Match(&tags))

	tags = element.Tags{"highway": "footway", "landuse": "park"}
	// landusages not a linestring table
	matchesEqual(t, []Match{{"highway", "footway", DestTable{"roads", "roads"}, nil}}, ls.Match(&tags))
}

func TestPolygonMatcher(t *testing.T) {
	var tags element.Tags
	polys := mapping.PolygonMatcher()

	tags = element.Tags{"unknown": "baz"}
	matchesEqual(t, []Match{}, polys.Match(&tags))

	tags = element.Tags{"landuse": "unknowns"}
	matchesEqual(t, []Match{}, polys.Match(&tags))

	tags = element.Tags{"building": "yes"}
	matchesEqual(t, []Match{{"building", "yes", DestTable{"buildings", ""}, nil}}, polys.Match(&tags))
	tags = element.Tags{"building": "residential"}
	matchesEqual(t, []Match{{"building", "residential", DestTable{"buildings", ""}, nil}}, polys.Match(&tags))

	tags = element.Tags{"landuse": "farm"}
	matchesEqual(t, []Match{{"landuse", "farm", DestTable{"landusages", ""}, nil}}, polys.Match(&tags))

	tags = element.Tags{"landuse": "farm", "highway": "secondary"}
	matchesEqual(t, []Match{{"landuse", "farm", DestTable{"landusages", ""}, nil}}, polys.Match(&tags))

	tags = element.Tags{"landuse": "farm", "aeroway": "apron"}
	matchesEqual(t,
		[]Match{
			{"aeroway", "apron", DestTable{"transport_areas", ""}, nil},
			{"landuse", "farm", DestTable{"landusages", ""}, nil}},
		polys.Match(&tags))

	tags = element.Tags{"highway": "footway"}
	matchesEqual(t, []Match{{"highway", "footway", DestTable{"landusages", ""}, nil}}, polys.Match(&tags))

	tags = element.Tags{"boundary": "administrative", "admin_level": "8"}
	matchesEqual(t, []Match{{"boundary", "administrative", DestTable{"admin", ""}, nil}}, polys.Match(&tags))
}

func TestFilterNodes(t *testing.T) {
	var tags element.Tags

	// test name only
	tags = make(element.Tags)
	tags["name"] = "foo"

	points := mapping.NodeTagFilter()
	if points.Filter(&tags) != false {
		t.Fatal("Filter result not false")
	}
	if len(tags) != 0 {
		t.Fatal("Filter result not empty")
	}

	// test name + unmapped tags
	tags = make(element.Tags)
	tags["name"] = "foo"
	tags["boring"] = "true"

	if points.Filter(&tags) != false {
		t.Fatal("Filter result not false")
	}
	if len(tags) != 0 {
		t.Fatal("Filter result not empty")
	}

	// test fields only, but no mapping
	tags = make(element.Tags)
	tags["population"] = "0"
	tags["name"] = "foo"
	tags["boring"] = "true"

	if points.Filter(&tags) != false {
		t.Fatal("Filter result true", tags)
	}
	if len(tags) != 0 {
		t.Fatal("Filter result not empty", tags)
	}

	// ... not with mapped tag (place)
	tags = make(element.Tags)
	tags["population"] = "0"
	tags["name"] = "foo"
	tags["boring"] = "true"
	tags["place"] = "village"

	if points.Filter(&tags) != true {
		t.Fatal("Filter result true", tags)
	}
	if len(tags) != 3 && tags["population"] == "0" && tags["name"] == "foo" && tags["place"] == "village" {
		t.Fatal("Filter result not expected", tags)
	}
}

func BenchmarkFilterNodes(b *testing.B) {
	var tags element.Tags

	for i := 0; i < b.N; i++ {
		// test __any__
		tags = make(element.Tags)
		tags["population"] = "0"
		tags["name"] = "foo"
		tags["boring"] = "true"

		points := mapping.NodeTagFilter()
		if points.Filter(&tags) != true {
			b.Fatal("Filter result true", tags)
		}
		if len(tags) != 2 && tags["population"] == "0" && tags["name"] == "foo" {
			b.Fatal("Filter result not expected", tags)
		}
	}

}
