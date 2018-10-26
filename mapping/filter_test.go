package mapping

import (
	"reflect"
	"testing"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/mapping/config"
)

func TestTagFilterNodes(t *testing.T) {
	mapping, err := New([]byte(`
    tables:
      places:
        type: point
        columns:
        - key: name
          name: name
          type: string
        - args:
            values:
            - village
            - town
            - city
            - county
          name: z_order
          type: enumerate
        - key: population
          name: population
          type: integer
        mapping:
          place:
          - city
          - town
          - village
      transport_points:
        type: point
        columns:
        mapping:
          highway: [bus_stop]
      highways:
        type: linestring
        mapping:
          highway: [__any__] # ignored as we test node filters
    `))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		tags     osm.Tags
		expected osm.Tags
	}{
		{tags: osm.Tags{}, expected: osm.Tags{}},
		{tags: osm.Tags{"name": "foo"}, expected: osm.Tags{"name": "foo"}},
		{tags: osm.Tags{"name": "foo", "unknown": "foo"}, expected: osm.Tags{"name": "foo"}},
		{tags: osm.Tags{"place": "unknown"}, expected: osm.Tags{}},
		{tags: osm.Tags{"place": "village"}, expected: osm.Tags{"place": "village"}},
		{tags: osm.Tags{"population": "1000"}, expected: osm.Tags{"population": "1000"}},
		{tags: osm.Tags{"highway": "bus_stop"}, expected: osm.Tags{"highway": "bus_stop"}},
		{tags: osm.Tags{"highway": "residential"}, expected: osm.Tags{}},
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
	mapping, err := New([]byte(`
    tables:
      buildings:
        type: polygon
        mapping:
          building: [__any__]
      highways:
        type: linestring
        columns:
            - key: name
              name: name
              type: string
            - key: tunnel
              name: tunnel
              type: boolint
            - key: oneway
              name: oneway
              type: direction
        mapping:
          highway:
            - track
      places:
        type: point
        mapping:
          place:  # ignored as we test ways
          - city
          - town
          - village
    `))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		tags     osm.Tags
		expected osm.Tags
	}{
		{tags: osm.Tags{}, expected: osm.Tags{}},
		{tags: osm.Tags{"name": "foo"}, expected: osm.Tags{"name": "foo"}},
		{tags: osm.Tags{"name": "foo", "unknown": "foo"}, expected: osm.Tags{"name": "foo"}},
		{tags: osm.Tags{"highway": "unknown"}, expected: osm.Tags{}},
		{tags: osm.Tags{"highway": "track"}, expected: osm.Tags{"highway": "track"}},
		{tags: osm.Tags{"building": "whatever"}, expected: osm.Tags{"building": "whatever"}},
		{tags: osm.Tags{"place": "village"}, expected: osm.Tags{}},
		{tags: osm.Tags{"oneway": "yes", "tunnel": "1"}, expected: osm.Tags{"oneway": "yes", "tunnel": "1"}},
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
	mapping, err := New([]byte(`
    tags:
        include: [source]
    tables:
      landuse:
        type: polygon
        mapping:
          landuse: [farm]
      buildings:
        type: polygon
        columns:
          - key: name
            type: string
            name: name
        mapping:
          building: [__any__]
      highways:
        type: linestring
        mapping:
          highway: # imported for relations
            - track
      places:
        type: point
        mapping:
          place: # ignored as we test ways
          - city
          - town
          - village
    `))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		tags     osm.Tags
		expected osm.Tags
	}{
		{tags: osm.Tags{}, expected: osm.Tags{}},
		{tags: osm.Tags{"name": "foo"}, expected: osm.Tags{"name": "foo"}},
		{tags: osm.Tags{"unknown": "foo"}, expected: osm.Tags{}},
		{tags: osm.Tags{"landuse": "unknown"}, expected: osm.Tags{}},
		{tags: osm.Tags{"highway": "track"}, expected: osm.Tags{"highway": "track"}},
		{tags: osm.Tags{"place": "town"}, expected: osm.Tags{}},
		{tags: osm.Tags{"landuse": "farm"}, expected: osm.Tags{"landuse": "farm"}},
		{tags: osm.Tags{"landuse": "farm", "type": "multipolygon"}, expected: osm.Tags{"landuse": "farm", "type": "multipolygon"}},
		{tags: osm.Tags{"type": "multipolygon"}, expected: osm.Tags{"type": "multipolygon"}},
		{tags: osm.Tags{"type": "boundary"}, expected: osm.Tags{"type": "boundary"}},
		{tags: osm.Tags{"building": "yes"}, expected: osm.Tags{"building": "yes"}},
		{tags: osm.Tags{"source": "JOSM"}, expected: osm.Tags{"source": "JOSM"}},
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
	mapping, err := New([]byte(`
    tables:
      places:
        type: point
        columns:
        - key: name
          name: name
          type: string
        - args:
            values:
            - village
            - town
            - city
            - county
          name: z_order
          type: enumerate
        - key: population
          name: population
          type: integer
        mapping:
          place:
          - city
          - town
          - village
      transport_points:
        type: point
        columns:
        mapping:
          highway: [bus_stop]
      highways:
        type: linestring
        mapping:
          highway: [__any__] # ignored as we test node filters
    `))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		tags    osm.Tags
		matches []Match
	}{
		{osm.Tags{"unknown": "baz"}, []Match{}},
		{osm.Tags{"place": "unknown"}, []Match{}},
		{osm.Tags{"place": "city"}, []Match{{"place", "city", DestTable{Name: "places"}, nil}}},
		{osm.Tags{"place": "city", "highway": "residential"}, []Match{{"place", "city", DestTable{Name: "places"}, nil}}},
		{osm.Tags{"place": "city", "highway": "bus_stop"}, []Match{
			{"place", "city", DestTable{Name: "places"}, nil},
			{"highway", "bus_stop", DestTable{Name: "transport_points"}, nil}},
		},
	}

	elem := osm.Node{}
	m := mapping.PointMatcher
	for i, test := range tests {
		elem.Tags = test.tags
		actual := m.MatchNode(&elem)
		if !matchesEqual(actual, test.matches) {
			t.Errorf("unexpected result for case %d: %v != %v", i+1, actual, test.matches)
		}
	}
}

func TestLineStringMatcher(t *testing.T) {
	mapping, err := New([]byte(`
    areas:
      area_tags: [buildings, landuse, leisure, natural, aeroway]
      linear_tags: [highway, barrier]
    tables:
      landuse:
        type: polygon
        mapping:
          landuse: [park]
      aeroways:
        type: linestring
        mapping:
          aeroway: [runway]
      barrierways:
        type: linestring
        mapping:
          barrier: [hedge]
      roads:
        type: linestring
        mappings:
          roads:
            mapping:
              highway:
                - track
                - pedestrian
                - footway
                - secondary
          railway:
            mapping:
              railway:
                - tram
      places:
        type: point
        mapping:
          place:  # ignored as we test ways
          - city
          - town
          - village
    `))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		tags    osm.Tags
		matches []Match
	}{
		{osm.Tags{"unknown": "baz"}, []Match{}},
		{osm.Tags{"highway": "unknown"}, []Match{}},
		{osm.Tags{"place": "city"}, []Match{}},
		{osm.Tags{"highway": "pedestrian"},
			[]Match{{"highway", "pedestrian", DestTable{Name: "roads", SubMapping: "roads"}, nil}}},

		// exclude_tags area=yes
		{osm.Tags{"highway": "pedestrian", "area": "yes"}, []Match{}},

		{osm.Tags{"barrier": "hedge"},
			[]Match{{"barrier", "hedge", DestTable{Name: "barrierways"}, nil}}},
		{osm.Tags{"barrier": "hedge", "area": "yes"}, []Match{}},

		{osm.Tags{"aeroway": "runway"}, []Match{}},
		{osm.Tags{"aeroway": "runway", "area": "no"},
			[]Match{{"aeroway", "runway", DestTable{Name: "aeroways"}, nil}}},

		{osm.Tags{"highway": "secondary", "railway": "tram"},
			[]Match{
				{"highway", "secondary", DestTable{Name: "roads", SubMapping: "roads"}, nil},
				{"railway", "tram", DestTable{Name: "roads", SubMapping: "railway"}, nil}},
		},
		{osm.Tags{"highway": "footway", "landuse": "park", "barrier": "hedge"},
			// landusages not a linestring table
			[]Match{
				{"highway", "footway", DestTable{Name: "roads", SubMapping: "roads"}, nil},
				{"barrier", "hedge", DestTable{Name: "barrierways"}, nil}},
		},
	}

	elem := osm.Way{}
	// fake closed way for area matching
	elem.Refs = []int64{1, 2, 3, 4, 1}
	if !elem.IsClosed() {
		t.Fatal("way not closed")
	}
	m := mapping.LineStringMatcher
	for i, test := range tests {
		elem.Tags = test.tags
		actual := m.MatchWay(&elem)
		if !matchesEqual(actual, test.matches) {
			t.Errorf("unexpected result for case %d: %v != %v", i+1, actual, test.matches)
		}
	}
}

func TestPolygonMatcher_MatchWay(t *testing.T) {
	mapping, err := New([]byte(`
    areas:
      area_tags: [buildings, landuse, leisure, natural, aeroway]
      linear_tags: [highway, barrier]
    tables:
      landusages:
        type: polygon
        mapping:
            amenity: [university]
            landuse: [farm, forest]
            leisure: [park]
            landuse: [park]
            highway: [footway]
            barrier: [hedge]
      transport_areas:
        type: polygon
        mapping:
          aeroway: [runway, apron]
      barrierways:
        type: linestring
        mapping:
          barrier: [hedge]
      buildings:
        type: polygon
        mapping:
          building: [__any__]
      amenity_areas:
        type: polygon
        mapping:
          building: [shop]
      highways:
        type: linestring
        mapping:
          highway: # imported for relations
            - track
      admin:
        type: polygon
        mapping:
          boundary: [administrative]
    `))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		tags    osm.Tags
		matches []Match
	}{
		{osm.Tags{}, []Match{}},
		{osm.Tags{"unknown": "baz"}, []Match{}},
		{osm.Tags{"landuse": "unknown"}, []Match{}},
		{osm.Tags{"landuse": "unknown", "type": "multipolygon"}, []Match{}},
		{osm.Tags{"building": "yes"}, []Match{{"building", "yes", DestTable{Name: "buildings"}, nil}}},
		{osm.Tags{"building": "residential"}, []Match{{"building", "residential", DestTable{Name: "buildings"}, nil}}},
		// line type requires area=yes
		{osm.Tags{"barrier": "hedge"}, []Match{}},
		{osm.Tags{"barrier": "hedge", "area": "yes"}, []Match{{"barrier", "hedge", DestTable{Name: "landusages"}, nil}}},

		{osm.Tags{"building": "shop"}, []Match{
			{"building", "shop", DestTable{Name: "buildings"}, nil},
			{"building", "shop", DestTable{Name: "amenity_areas"}, nil},
		}},

		{osm.Tags{"aeroway": "apron", "landuse": "farm"}, []Match{
			{"aeroway", "apron", DestTable{Name: "transport_areas"}, nil},
			{"landuse", "farm", DestTable{Name: "landusages"}, nil},
		}},

		{osm.Tags{"landuse": "farm", "highway": "secondary"}, []Match{
			{"landuse", "farm", DestTable{Name: "landusages"}, nil},
		}},

		{osm.Tags{"highway": "footway"}, []Match{}},
		{osm.Tags{"highway": "footway", "area": "yes"}, []Match{
			{"highway", "footway", DestTable{Name: "landusages"}, nil},
		}},

		{osm.Tags{"boundary": "administrative", "admin_level": "8"}, []Match{{"boundary", "administrative", DestTable{Name: "admin"}, nil}}},

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

		{osm.Tags{"landuse": "forest", "leisure": "park"}, []Match{{"landuse", "forest", DestTable{Name: "landusages"}, nil}}},
		{osm.Tags{"landuse": "park", "leisure": "park"}, []Match{{"leisure", "park", DestTable{Name: "landusages"}, nil}}},
		{osm.Tags{"landuse": "park", "leisure": "park", "amenity": "university"}, []Match{{"amenity", "university", DestTable{Name: "landusages"}, nil}}},
	}

	elem := osm.Way{}
	// fake closed way for area matching
	elem.Refs = []int64{1, 2, 3, 4, 1}
	if !elem.IsClosed() {
		t.Fatal("way not closed")
	}
	m := mapping.PolygonMatcher
	for i, test := range tests {
		elem.Tags = test.tags
		actual := m.MatchWay(&elem)
		if !matchesEqual(actual, test.matches) {
			t.Errorf("unexpected result for case %d: %v != %v", i+1, actual, test.matches)
		}
	}

	elem.Refs = nil
	elem.Tags = osm.Tags{"building": "yes"}
	actual := m.MatchWay(&elem)
	if !matchesEqual([]Match{}, actual) {
		t.Error("open way matched as polygon")
	}
}

func TestPolygonMatcher_MatchRelation(t *testing.T) {
	// check that only relations with type=multipolygon/boundary are matched as polygon

	mapping, err := New([]byte(`
    areas:
      area_tags: [buildings, landuse, leisure, natural, aeroway]
      linear_tags: [highway, barrier]
    tables:
      landusages:
        type: polygon
        mapping:
            amenity: [university]
            landuse: [farm, forest]
            leisure: [park]
            landuse: [park]
            highway: [footway]
            barrier: [hedge]
      transport_areas:
        type: polygon
        mapping:
          aeroway: [runway, apron]
      barrierways:
        type: linestring
        mapping:
          barrier: [hedge]
      buildings:
        type: polygon
        mapping:
          building: [__any__]
      amenity_areas:
        type: polygon
        mapping:
          building: [shop]
      highways:
        type: linestring
        mapping:
          highway: # imported for relations
            - track
      admin:
        type: polygon
        mapping:
          boundary: [administrative]
    `))
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		tags    osm.Tags
		matches []Match
	}{
		{osm.Tags{}, []Match{}},
		{osm.Tags{"unknown": "baz"}, []Match{}},
		{osm.Tags{"landuse": "unknown"}, []Match{}},
		{osm.Tags{"landuse": "unknown", "type": "multipolygon"}, []Match{}},
		{osm.Tags{"building": "yes"}, []Match{}},
		{osm.Tags{"building": "yes", "type": "multipolygon"}, []Match{{"building", "yes", DestTable{Name: "buildings"}, nil}}},
		{osm.Tags{"building": "residential", "type": "multipolygon"}, []Match{{"building", "residential", DestTable{Name: "buildings"}, nil}}},
		// line type requires area=yes
		{osm.Tags{"barrier": "hedge", "type": "multipolygon"}, []Match{}},
		{osm.Tags{"barrier": "hedge", "area": "yes", "type": "multipolygon"}, []Match{{"barrier", "hedge", DestTable{Name: "landusages"}, nil}}},

		{osm.Tags{"building": "shop", "type": "multipolygon"}, []Match{
			{"building", "shop", DestTable{Name: "buildings"}, nil},
			{"building", "shop", DestTable{Name: "amenity_areas"}, nil},
		}},

		{osm.Tags{"aeroway": "apron", "landuse": "farm", "type": "multipolygon"}, []Match{
			{"aeroway", "apron", DestTable{Name: "transport_areas"}, nil},
			{"landuse", "farm", DestTable{Name: "landusages"}, nil},
		}},

		{osm.Tags{"landuse": "farm", "highway": "secondary", "type": "multipolygon"}, []Match{
			{"landuse", "farm", DestTable{Name: "landusages"}, nil},
		}},

		{osm.Tags{"highway": "footway", "type": "multipolygon"}, []Match{}},
		{osm.Tags{"highway": "footway", "area": "yes", "type": "multipolygon"}, []Match{
			{"highway", "footway", DestTable{Name: "landusages"}, nil},
		}},

		{osm.Tags{"boundary": "administrative", "admin_level": "8"}, []Match{}},
		{osm.Tags{"boundary": "administrative", "admin_level": "8", "type": "boundary"}, []Match{{"boundary", "administrative", DestTable{Name: "admin"}, nil}}},
	}

	elem := osm.Relation{}
	m := mapping.PolygonMatcher
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
	var tags osm.Tags

	// no matches
	f = newExcludeFilter([]config.Key{})
	tags = osm.Tags{"source": "1", "tiger:foo": "1", "source:foo": "1"}
	f.Filter(&tags)
	if !reflect.DeepEqual(tags, osm.Tags{"source": "1", "tiger:foo": "1", "source:foo": "1"}) {
		t.Error("unexpected filter result", tags)
	}

	// match all
	f = newExcludeFilter([]config.Key{"*"})
	tags = osm.Tags{"source": "1", "tiger:foo": "1", "source:foo": "1"}
	f.Filter(&tags)
	if !reflect.DeepEqual(tags, osm.Tags{}) {
		t.Error("unexpected filter result", tags)
	}

	// fixed string and wildcard match
	f = newExcludeFilter([]config.Key{"source", "tiger:*"})
	tags = osm.Tags{"source": "1", "tiger:foo": "1", "source:foo": "1"}
	f.Filter(&tags)
	if !reflect.DeepEqual(tags, osm.Tags{"source:foo": "1"}) {
		t.Error("unexpected filter result", tags)
	}
}

func BenchmarkFilterNodes(b *testing.B) {
	var tags osm.Tags

	mapping, err := FromFile("./test_mapping.yml")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// test __any__
		tags = make(osm.Tags)
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
