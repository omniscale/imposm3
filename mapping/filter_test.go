package mapping

import (
	"reflect"
	"testing"

	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/mapping/config"
)

var mapping *Mapping

func init() {
	var err error
	mapping, err = NewMapping("./test_mapping.yml")
	if err != nil {
		panic(err)
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

func matchesEqual(expected []Match, actual []Match) bool {
	expectedMatches := make(map[DestTable]Match)
	actualMatches := make(map[DestTable]Match)

	if len(expected) != len(actual) {
		return false
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
				return false
			}
		} else {
			return false
		}
	}
	return true
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
	tests := []struct {
		tags    element.Tags
		matches []Match
	}{
		{element.Tags{"unknown": "baz"}, []Match{}},
		{element.Tags{"place": "unknown"}, []Match{}},
		{element.Tags{"place": "city"}, []Match{{"place", "city", DestTable{Name: "places"}, nil}}},
		{element.Tags{"place": "city", "highway": "unknown"}, []Match{{"place", "city", DestTable{Name: "places"}, nil}}},
		{element.Tags{"place": "city", "highway": "bus_stop"}, []Match{
			{"place", "city", DestTable{Name: "places"}, nil},
			{"highway", "bus_stop", DestTable{Name: "transport_points"}, nil}},
		},
	}

	elem := element.Node{}
	m := mapping.PointMatcher()
	for i, test := range tests {
		elem.Tags = test.tags
		actual := m.MatchNode(&elem)
		if !matchesEqual(actual, test.matches) {
			t.Errorf("unexpected result for case %d: %v != %v", i+1, actual, test.matches)
		}
	}
}

func TestLineStringMatcher(t *testing.T) {
	tests := []struct {
		tags    element.Tags
		matches []Match
	}{
		{element.Tags{"unknown": "baz"}, []Match{}},
		{element.Tags{"highway": "unknown"}, []Match{}},
		{element.Tags{"highway": "pedestrian"},
			[]Match{{"highway", "pedestrian", DestTable{Name: "roads", SubMapping: "roads"}, nil}}},

		// exclude_tags area=yes
		{element.Tags{"highway": "pedestrian", "area": "yes"}, []Match{}},

		{element.Tags{"barrier": "hedge"},
			[]Match{{"barrier", "hedge", DestTable{Name: "barrierways"}, nil}}},
		{element.Tags{"barrier": "hedge", "area": "yes"}, []Match{}},

		{element.Tags{"aeroway": "runway"}, []Match{}},
		{element.Tags{"aeroway": "runway", "area": "no"},
			[]Match{{"aeroway", "runway", DestTable{Name: "aeroways"}, nil}}},

		{element.Tags{"highway": "secondary", "railway": "tram"},
			[]Match{
				{"highway", "secondary", DestTable{Name: "roads", SubMapping: "roads"}, nil},
				{"railway", "tram", DestTable{Name: "roads", SubMapping: "railway"}, nil}},
		},
		{element.Tags{"highway": "footway", "landuse": "park", "barrier": "hedge"},
			// landusages not a linestring table
			[]Match{
				{"highway", "footway", DestTable{Name: "roads", SubMapping: "roads"}, nil},
				{"barrier", "hedge", DestTable{Name: "barrierways"}, nil}},
		},
	}

	elem := element.Way{}
	// fake closed way for area matching
	elem.Refs = []int64{1, 2, 3, 4, 1}
	if !elem.IsClosed() {
		t.Fatal("way not closed")
	}
	m := mapping.LineStringMatcher()
	for i, test := range tests {
		elem.Tags = test.tags
		actual := m.MatchWay(&elem)
		if !matchesEqual(actual, test.matches) {
			t.Errorf("unexpected result for case %d: %v != %v", i+1, actual, test.matches)
		}
	}
}

func TestPolygonMatcher_MatchWay(t *testing.T) {
	tests := []struct {
		tags    element.Tags
		matches []Match
	}{
		{element.Tags{}, []Match{}},
		{element.Tags{"unknown": "baz"}, []Match{}},
		{element.Tags{"landuse": "unknown"}, []Match{}},
		{element.Tags{"landuse": "unknown", "type": "multipolygon"}, []Match{}},
		{element.Tags{"building": "yes"}, []Match{{"building", "yes", DestTable{Name: "buildings"}, nil}}},
		{element.Tags{"building": "residential"}, []Match{{"building", "residential", DestTable{Name: "buildings"}, nil}}},
		// line type requires area=yes
		{element.Tags{"barrier": "hedge"}, []Match{}},
		{element.Tags{"barrier": "hedge", "area": "yes"}, []Match{{"barrier", "hedge", DestTable{Name: "landusages"}, nil}}},

		{element.Tags{"building": "shop"}, []Match{
			{"building", "shop", DestTable{Name: "buildings"}, nil},
			{"building", "shop", DestTable{Name: "amenity_areas"}, nil},
		}},

		{element.Tags{"aeroway": "apron", "landuse": "farm"}, []Match{
			{"aeroway", "apron", DestTable{Name: "transport_areas"}, nil},
			{"landuse", "farm", DestTable{Name: "landusages"}, nil},
		}},

		{element.Tags{"landuse": "farm", "highway": "secondary"}, []Match{
			{"landuse", "farm", DestTable{Name: "landusages"}, nil},
		}},

		{element.Tags{"highway": "footway"}, []Match{}},
		{element.Tags{"highway": "footway", "area": "yes"}, []Match{
			{"highway", "footway", DestTable{Name: "landusages"}, nil},
		}},

		{element.Tags{"boundary": "administrative", "admin_level": "8"}, []Match{{"boundary", "administrative", DestTable{Name: "admin"}, nil}}},

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

		{element.Tags{"landuse": "forest", "leisure": "park"}, []Match{{"landuse", "forest", DestTable{Name: "landusages"}, nil}}},
		{element.Tags{"landuse": "park", "leisure": "park"}, []Match{{"leisure", "park", DestTable{Name: "landusages"}, nil}}},
		{element.Tags{"landuse": "park", "leisure": "park", "amenity": "university"}, []Match{{"amenity", "university", DestTable{Name: "landusages"}, nil}}},
	}

	elem := element.Way{}
	// fake closed way for area matching
	elem.Refs = []int64{1, 2, 3, 4, 1}
	if !elem.IsClosed() {
		t.Fatal("way not closed")
	}
	m := mapping.PolygonMatcher()
	for i, test := range tests {
		elem.Tags = test.tags
		actual := m.MatchWay(&elem)
		if !matchesEqual(actual, test.matches) {
			t.Errorf("unexpected result for case %d: %v != %v", i+1, actual, test.matches)
		}
	}

	elem.Refs = nil
	elem.Tags = element.Tags{"building": "yes"}
	actual := m.MatchWay(&elem)
	if !matchesEqual([]Match{}, actual) {
		t.Error("open way matched as polygon")
	}
}

func TestPolygonMatcher_MatchRelation(t *testing.T) {
	// check that only relations with type=multipolygon/boundary are matched as polygon

	tests := []struct {
		tags    element.Tags
		matches []Match
	}{
		{element.Tags{}, []Match{}},
		{element.Tags{"unknown": "baz"}, []Match{}},
		{element.Tags{"landuse": "unknown"}, []Match{}},
		{element.Tags{"landuse": "unknown", "type": "multipolygon"}, []Match{}},
		{element.Tags{"building": "yes"}, []Match{}},
		{element.Tags{"building": "yes", "type": "multipolygon"}, []Match{{"building", "yes", DestTable{Name: "buildings"}, nil}}},
		{element.Tags{"building": "residential", "type": "multipolygon"}, []Match{{"building", "residential", DestTable{Name: "buildings"}, nil}}},
		// line type requires area=yes
		{element.Tags{"barrier": "hedge", "type": "multipolygon"}, []Match{}},
		{element.Tags{"barrier": "hedge", "area": "yes", "type": "multipolygon"}, []Match{{"barrier", "hedge", DestTable{Name: "landusages"}, nil}}},

		{element.Tags{"building": "shop", "type": "multipolygon"}, []Match{
			{"building", "shop", DestTable{Name: "buildings"}, nil},
			{"building", "shop", DestTable{Name: "amenity_areas"}, nil},
		}},

		{element.Tags{"aeroway": "apron", "landuse": "farm", "type": "multipolygon"}, []Match{
			{"aeroway", "apron", DestTable{Name: "transport_areas"}, nil},
			{"landuse", "farm", DestTable{Name: "landusages"}, nil},
		}},

		{element.Tags{"landuse": "farm", "highway": "secondary", "type": "multipolygon"}, []Match{
			{"landuse", "farm", DestTable{Name: "landusages"}, nil},
		}},

		{element.Tags{"highway": "footway", "type": "multipolygon"}, []Match{}},
		{element.Tags{"highway": "footway", "area": "yes", "type": "multipolygon"}, []Match{
			{"highway", "footway", DestTable{Name: "landusages"}, nil},
		}},

		{element.Tags{"boundary": "administrative", "admin_level": "8"}, []Match{}},
		{element.Tags{"boundary": "administrative", "admin_level": "8", "type": "boundary"}, []Match{{"boundary", "administrative", DestTable{Name: "admin"}, nil}}},
	}

	elem := element.Relation{}
	m := mapping.PolygonMatcher()
	for i, test := range tests {
		elem.Tags = test.tags
		actual := m.MatchRelation(&elem)
		if !matchesEqual(actual, test.matches) {
			t.Errorf("unexpected result for case %d: %v != %v", i+1, actual, test.matches)
		}
	}
}

func TestExcludeFilter(t *testing.T) {
	var f TagFilterer
	var tags element.Tags

	// no matches
	f = newExcludeFilter([]config.Key{})
	tags = element.Tags{"source": "1", "tiger:foo": "1", "source:foo": "1"}
	f.Filter(&tags)
	if !reflect.DeepEqual(tags, element.Tags{"source": "1", "tiger:foo": "1", "source:foo": "1"}) {
		t.Error("unexpected filter result", tags)
	}

	// match all
	f = newExcludeFilter([]config.Key{"*"})
	tags = element.Tags{"source": "1", "tiger:foo": "1", "source:foo": "1"}
	f.Filter(&tags)
	if !reflect.DeepEqual(tags, element.Tags{}) {
		t.Error("unexpected filter result", tags)
	}

	// fixed string and wildcard match
	f = newExcludeFilter([]config.Key{"source", "tiger:*"})
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
