package mapping

import (
	"goposm/element"
	"testing"
)

var mapping *Mapping

func init() {
	var err error
	mapping, err = NewMapping("../mapping.json")
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
	expectedMatches := make(map[string]Match)
	actualMatches := make(map[string]Match)

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

	tags = element.Tags{"name": "foo", "place": "village"}
	if nodes.Filter(&tags) != true {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{"name": "foo", "place": "village"}, tags)

	// TODO
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

	// TODO
}

func TestTagFilterRelations(t *testing.T) {
	var tags element.Tags
	relations := mapping.RelationTagFilter()

	tags = element.Tags{"name": "foo"}
	if relations.Filter(&tags) != false {
		t.Fatal("unexpected filter response for", tags)
	}
	stringMapEquals(t, element.Tags{}, tags)
	// TODO
}

func TestPointMatcher(t *testing.T) {
	var tags element.Tags
	points := mapping.PointMatcher()

	tags = element.Tags{"unknown": "baz"}
	matchesEqual(t, []Match{}, points.Match(&tags))

	tags = element.Tags{"place": "unknown"}
	matchesEqual(t, []Match{}, points.Match(&tags))

	tags = element.Tags{"place": "city"}
	matchesEqual(t, []Match{{"place", "city", "places", nil}}, points.Match(&tags))

	// TODO
}

func TestLineStringMatcher(t *testing.T) {
	var tags element.Tags
	ls := mapping.LineStringMatcher()

	tags = element.Tags{"unknown": "baz"}
	matchesEqual(t, []Match{}, ls.Match(&tags))

	// TODO
}

func TestPolygonMatcher(t *testing.T) {
	var tags element.Tags
	polys := mapping.PolygonMatcher()

	tags = element.Tags{"unknown": "baz"}
	matchesEqual(t, []Match{}, polys.Match(&tags))

	// TODO
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
