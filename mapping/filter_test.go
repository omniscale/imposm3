package mapping

import (
	"reflect"
	"testing"

	"github.com/omniscale/imposm3/element"
)

var mapping *Mapping

func init() {
	var err error
	mapping, err = NewMapping("./test_mapping.yml")
	if err != nil {
		panic(err)
	}
}

func stringMapEquals(t *testing.T, expected, actual map[string]string) {
	if len(expected) != len(actual) {
		t.Errorf("different length in %v and %v\n", expected, actual)
	}

	for k, v := range expected {
		if actualV, ok := actual[k]; ok {
			if actualV != v {
				t.Errorf("%s != %s in %v and %v\n", v, actualV, expected, actual)
			}
		} else {
			t.Errorf("%s not in %v\n", k, actual)
		}
	}
}

func stringMapEqual(expected, actual map[string]string) bool {
	if len(expected) != len(actual) {
		return false
	}

	for k, v := range expected {
		if actualV, ok := actual[k]; ok {
			if actualV != v {
				return false
			}
		} else {
			return false
		}
	}
	return true
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
	tests := []struct {
		tags     element.Tags
		expected element.Tags
	}{
		{tags: element.Tags{}, expected: element.Tags{}},
		{tags: element.Tags{"name": "foo"}, expected: element.Tags{"name": "foo"}},
		{tags: element.Tags{"name": "foo", "unknown": "foo"}, expected: element.Tags{"name": "foo"}},
		{tags: element.Tags{"name": "foo", "place": "unknown"}, expected: element.Tags{"name": "foo"}},
		{tags: element.Tags{"name": "foo", "place": "unknown", "population": "1000"}, expected: element.Tags{"name": "foo", "population": "1000"}},
		{tags: element.Tags{"name": "foo", "place": "village"}, expected: element.Tags{"name": "foo", "place": "village"}},
		{tags: element.Tags{"name": "foo", "place": "village", "population": "1000"}, expected: element.Tags{"name": "foo", "place": "village", "population": "1000"}},
		{tags: element.Tags{"name": "foo", "place": "village", "unknown": "foo"}, expected: element.Tags{"name": "foo", "place": "village"}},
		{tags: element.Tags{"name": "foo", "place": "village", "highway": "bus_stop"}, expected: element.Tags{"name": "foo", "place": "village", "highway": "bus_stop"}},
	}

	nodes := mapping.NodeTagFilter()
	for i, test := range tests {
		nodes.Filter(&test.tags)
		if !stringMapEqual(test.tags, test.expected) {
			t.Errorf("unexpected result for case %d: %v != %v", i+1, test.tags, test.expected)
		}
	}
}

func TestTagFilterWays(t *testing.T) {
	tests := []struct {
		tags     element.Tags
		expected element.Tags
	}{
		{tags: element.Tags{}, expected: element.Tags{}},
		{tags: element.Tags{"name": "foo"}, expected: element.Tags{"name": "foo"}},
		{tags: element.Tags{"name": "foo", "unknown": "foo"}, expected: element.Tags{"name": "foo"}},
		{tags: element.Tags{"name": "foo", "highway": "unknown"}, expected: element.Tags{"name": "foo"}},
		{tags: element.Tags{"name": "foo", "highway": "track"}, expected: element.Tags{"name": "foo", "highway": "track"}},
		{tags: element.Tags{"name": "foo", "building": "whatever"}, expected: element.Tags{"name": "foo", "building": "whatever"}},
		{tags: element.Tags{"name": "foo", "highway": "track", "unknown": "foo"}, expected: element.Tags{"name": "foo", "highway": "track"}},
		{tags: element.Tags{"name": "foo", "place": "village", "highway": "track"}, expected: element.Tags{"name": "foo", "highway": "track"}},
		{tags: element.Tags{"name": "foo", "highway": "track", "oneway": "yes", "tunnel": "1"}, expected: element.Tags{"name": "foo", "highway": "track", "oneway": "yes", "tunnel": "1"}},
	}

	ways := mapping.WayTagFilter()
	for i, test := range tests {
		ways.Filter(&test.tags)
		if !stringMapEqual(test.tags, test.expected) {
			t.Errorf("unexpected result for case %d: %v != %v", i+1, test.tags, test.expected)
		}
	}
}

func TestTagFilterRelations(t *testing.T) {
	tests := []struct {
		tags     element.Tags
		expected element.Tags
	}{
		{tags: element.Tags{}, expected: element.Tags{}},
		{tags: element.Tags{"name": "foo"}, expected: element.Tags{"name": "foo"}},
		{tags: element.Tags{"name": "foo", "unknown": "foo"}, expected: element.Tags{"name": "foo"}},
		{tags: element.Tags{"name": "foo", "landuse": "unknown"}, expected: element.Tags{"name": "foo"}},
		{tags: element.Tags{"name": "foo", "landuse": "farm"}, expected: element.Tags{"name": "foo", "landuse": "farm"}},
		{tags: element.Tags{"name": "foo", "landuse": "farm", "type": "multipolygon"}, expected: element.Tags{"name": "foo", "landuse": "farm", "type": "multipolygon"}},
		{tags: element.Tags{"name": "foo", "type": "multipolygon"}, expected: element.Tags{"name": "foo", "type": "multipolygon"}},
		{tags: element.Tags{"name": "foo", "type": "boundary"}, expected: element.Tags{"name": "foo", "type": "boundary"}},
		{tags: element.Tags{"name": "foo", "landuse": "farm", "type": "boundary"}, expected: element.Tags{"name": "foo", "landuse": "farm", "type": "boundary"}},
	}

	relations := mapping.RelationTagFilter()
	for i, test := range tests {
		relations.Filter(&test.tags)
		if !stringMapEqual(test.tags, test.expected) {
			t.Errorf("unexpected result for case %d: %v != %v", i+1, test.tags, test.expected)
		}
	}
}

func TestPointMatcher(t *testing.T) {
	elem := element.Node{}
	points := mapping.PointMatcher()

	elem.Tags = element.Tags{"unknown": "baz"}
	matchesEqual(t, []Match{}, points.MatchNode(&elem))

	elem.Tags = element.Tags{"place": "unknown"}
	matchesEqual(t, []Match{}, points.MatchNode(&elem))

	elem.Tags = element.Tags{"place": "city"}
	matchesEqual(t, []Match{{"place", "city", DestTable{Name: "places"}, nil}}, points.MatchNode(&elem))

	elem.Tags = element.Tags{"place": "city", "highway": "unknown"}
	matchesEqual(t, []Match{{"place", "city", DestTable{Name: "places"}, nil}}, points.MatchNode(&elem))

	elem.Tags = element.Tags{"place": "city", "highway": "bus_stop"}
	matchesEqual(t,
		[]Match{
			{"place", "city", DestTable{Name: "places"}, nil},
			{"highway", "bus_stop", DestTable{Name: "transport_points"}, nil}},
		points.MatchNode(&elem))
}

func TestLineStringMatcher(t *testing.T) {
	elem := element.Way{}
	// fake closed way for area matching
	elem.Refs = []int64{1, 2, 3, 4, 1}
	if !elem.IsClosed() {
		t.Fatal("way not closed")
	}
	ls := mapping.LineStringMatcher()

	elem.Tags = element.Tags{"unknown": "baz"}
	matchesEqual(t, []Match{}, ls.MatchWay(&elem))

	elem.Tags = element.Tags{"highway": "unknown"}
	matchesEqual(t, []Match{}, ls.MatchWay(&elem))

	elem.Tags = element.Tags{"highway": "pedestrian"}
	matchesEqual(t, []Match{{"highway", "pedestrian", DestTable{Name: "roads", SubMapping: "roads"}, nil}}, ls.MatchWay(&elem))

	// exclude_tags area=yes
	elem.Tags = element.Tags{"highway": "pedestrian", "area": "yes"}
	matchesEqual(t, []Match{}, ls.MatchWay(&elem))

	elem.Tags = element.Tags{"barrier": "hedge"}
	matchesEqual(t, []Match{{"barrier", "hedge", DestTable{Name: "barrierways"}, nil}}, ls.MatchWay(&elem))

	elem.Tags = element.Tags{"barrier": "hedge", "area": "yes"}
	matchesEqual(t, []Match{}, ls.MatchWay(&elem))

	elem.Tags = element.Tags{"aeroway": "runway", "area": "no"}
	matchesEqual(t, []Match{{"aeroway", "runway", DestTable{Name: "aeroways"}, nil}}, ls.MatchWay(&elem))

	elem.Tags = element.Tags{"aeroway": "runway"}
	matchesEqual(t, []Match{}, ls.MatchWay(&elem))

	elem.Tags = element.Tags{"highway": "secondary", "railway": "tram"}
	matchesEqual(t,
		[]Match{
			{"highway", "secondary", DestTable{Name: "roads", SubMapping: "roads"}, nil},
			{"railway", "tram", DestTable{Name: "roads", SubMapping: "railway"}, nil}},
		ls.MatchWay(&elem))

	elem.Tags = element.Tags{"highway": "footway", "landuse": "park"}
	// landusages not a linestring table
	matchesEqual(t, []Match{{"highway", "footway", DestTable{Name: "roads", SubMapping: "roads"}, nil}}, ls.MatchWay(&elem))
}

func TestPolygonMatcher(t *testing.T) {
	elem := element.Relation{}
	polys := mapping.PolygonMatcher()

	elem.Tags = element.Tags{"unknown": "baz"}
	matchesEqual(t, []Match{}, polys.MatchRelation(&elem))

	elem.Tags = element.Tags{"landuse": "unknowns"}
	matchesEqual(t, []Match{}, polys.MatchRelation(&elem))

	elem.Tags = element.Tags{"building": "yes"}
	matchesEqual(t, []Match{{"building", "yes", DestTable{Name: "buildings"}, nil}}, polys.MatchRelation(&elem))
	elem.Tags = element.Tags{"building": "residential"}
	matchesEqual(t, []Match{{"building", "residential", DestTable{Name: "buildings"}, nil}}, polys.MatchRelation(&elem))

	elem.Tags = element.Tags{"barrier": "hedge"}
	matchesEqual(t, []Match{}, polys.MatchRelation(&elem))

	elem.Tags = element.Tags{"barrier": "hedge", "area": "yes"}
	matchesEqual(t, []Match{{"barrier", "hedge", DestTable{Name: "landusages"}, nil}}, polys.MatchRelation(&elem))

	elem.Tags = element.Tags{"building": "shop"}
	matchesEqual(t, []Match{
		{"building", "shop", DestTable{Name: "buildings"}, nil},
		{"building", "shop", DestTable{Name: "amenity_areas"}, nil}},
		polys.MatchRelation(&elem))

	elem.Tags = element.Tags{"landuse": "farm"}
	matchesEqual(t, []Match{{"landuse", "farm", DestTable{Name: "landusages"}, nil}}, polys.MatchRelation(&elem))

	elem.Tags = element.Tags{"landuse": "farm", "highway": "secondary"}
	matchesEqual(t, []Match{{"landuse", "farm", DestTable{Name: "landusages"}, nil}}, polys.MatchRelation(&elem))

	elem.Tags = element.Tags{"landuse": "farm", "aeroway": "apron"}
	matchesEqual(t,
		[]Match{
			{"aeroway", "apron", DestTable{Name: "transport_areas"}, nil},
			{"landuse", "farm", DestTable{Name: "landusages"}, nil}},
		polys.MatchRelation(&elem))

	elem.Tags = element.Tags{"highway": "footway"} // linear by default
	matchesEqual(t, []Match{}, polys.MatchRelation(&elem))

	elem.Tags = element.Tags{"highway": "footway", "area": "yes"}
	matchesEqual(t, []Match{{"highway", "footway", DestTable{Name: "landusages"}, nil}}, polys.MatchRelation(&elem))

	elem.Tags = element.Tags{"boundary": "administrative", "admin_level": "8"}
	matchesEqual(t, []Match{{"boundary", "administrative", DestTable{Name: "admin"}, nil}}, polys.MatchRelation(&elem))
}

func TestMatcherMappingOrder(t *testing.T) {
	elem := element.Relation{}
	polys := mapping.PolygonMatcher()

	/*
		landusages mapping has the following order,
		check that XxxMatcher always uses the first

		amenity:
		- university
		landuse:
		- forest
		leisure:
		- park
		landuse:
		- park
	*/

	elem.Tags = element.Tags{"landuse": "forest", "leisure": "park"}
	matchesEqual(t, []Match{{"landuse", "forest", DestTable{Name: "landusages"}, nil}}, polys.MatchRelation(&elem))

	elem.Tags = element.Tags{"landuse": "park", "leisure": "park"}
	matchesEqual(t, []Match{{"leisure", "park", DestTable{Name: "landusages"}, nil}}, polys.MatchRelation(&elem))

	elem.Tags = element.Tags{"landuse": "park", "leisure": "park", "amenity": "university"}
	matchesEqual(t, []Match{{"amenity", "university", DestTable{Name: "landusages"}, nil}}, polys.MatchRelation(&elem))
}

func TestExcludeFilter(t *testing.T) {
	var f TagFilterer
	var tags element.Tags

	// no matches
	f = newExcludeFilter([]Key{})
	tags = element.Tags{"source": "1", "tiger:foo": "1", "source:foo": "1"}
	f.Filter(&tags)
	if !reflect.DeepEqual(tags, element.Tags{"source": "1", "tiger:foo": "1", "source:foo": "1"}) {
		t.Error("unexpected filter result", tags)
	}

	// match all
	f = newExcludeFilter([]Key{"*"})
	tags = element.Tags{"source": "1", "tiger:foo": "1", "source:foo": "1"}
	f.Filter(&tags)
	if !reflect.DeepEqual(tags, element.Tags{}) {
		t.Error("unexpected filter result", tags)
	}

	// fixed string and wildcard match
	f = newExcludeFilter([]Key{"source", "tiger:*"})
	tags = element.Tags{"source": "1", "tiger:foo": "1", "source:foo": "1"}
	f.Filter(&tags)
	if !reflect.DeepEqual(tags, element.Tags{"source:foo": "1"}) {
		t.Error("unexpected filter result", tags)
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
		points.Filter(&tags)
		if len(tags) != 2 && tags["population"] == "0" && tags["name"] == "foo" {
			b.Fatal("Filter result not expected", tags)
		}
	}
}
