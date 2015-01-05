package mapping

import (
	"testing"

	"github.com/omniscale/imposm3/element"
)

func BenchmarkTagMatch(b *testing.B) {
	m, err := NewMapping("matcher_test_mapping.json")
	if err != nil {
		b.Fatal(err)
	}
	matcher := m.PolygonMatcher()
	for i := 0; i < b.N; i++ {
		e := element.Relation{}
		e.Tags = element.Tags{"landuse": "forest", "name": "Forest", "source": "bling", "tourism": "zoo"}
		if m := matcher.MatchRelation(&e); len(m) != 1 {
			b.Fatal(m)
		}
	}
}

func makeMember(id int64, tags element.Tags) element.Member {
	way := &element.Way{
		element.OSMElem{id, tags, nil},
		[]int64{0, 1, 2, 0}, // fake closed way, req. for SelectRelationPolygons
		nil}
	return element.Member{Id: id, Type: element.WAY, Role: "", Way: way}
}

func makeMemberRole(id int64, tags element.Tags, role string) element.Member {
	way := &element.Way{
		element.OSMElem{id, tags, nil},
		[]int64{0, 1, 2, 0}, // fake closed way, req. for SelectRelationPolygons
		nil}
	return element.Member{Id: id, Type: element.WAY, Role: role, Way: way}
}

func TestSelectRelationPolygonsSimple(t *testing.T) {
	mapping, err := NewMapping("test_mapping.json")
	if err != nil {
		t.Fatal(err)
	}
	r := element.Relation{}
	r.Tags = element.Tags{"landuse": "park"}
	r.Members = []element.Member{
		makeMember(0, element.Tags{"landuse": "forest"}),
		makeMember(1, element.Tags{"landuse": "park"}),
		makeMember(2, element.Tags{"waterway": "riverbank"}),
		makeMember(4, element.Tags{"foo": "bar"}),
	}
	filtered := SelectRelationPolygons(
		mapping.PolygonMatcher(),
		&r,
	)
	if len(filtered) != 1 {
		t.Fatal(filtered)
	}
	if filtered[0].Id != 1 {
		t.Fatal(filtered[0])
	}
}

func TestSelectRelationPolygonsUnrelatedTags(t *testing.T) {
	mapping, err := NewMapping("test_mapping.json")
	if err != nil {
		t.Fatal(err)
	}
	r := element.Relation{}
	r.Tags = element.Tags{"landuse": "park"}
	r.Members = []element.Member{
		makeMember(0, element.Tags{"landuse": "park", "layer": "2", "name": "foo"}),
		makeMember(1, element.Tags{"landuse": "forest"}),
	}
	filtered := SelectRelationPolygons(
		mapping.PolygonMatcher(),
		&r,
	)
	if len(filtered) != 1 {
		t.Fatal(filtered)
	}
	if filtered[0].Id != 0 {
		t.Fatal(filtered)
	}
}

func TestSelectRelationPolygonsMultiple(t *testing.T) {
	mapping, err := NewMapping("test_mapping.json")
	if err != nil {
		t.Fatal(err)
	}
	r := element.Relation{}
	r.Tags = element.Tags{"landuse": "park"}
	r.Members = []element.Member{
		makeMember(0, element.Tags{"landuse": "park"}),
		makeMember(1, element.Tags{"natural": "forest"}),
		makeMember(2, element.Tags{"landuse": "park"}),
		makeMember(3, element.Tags{"highway": "pedestrian"}),
		makeMember(4, element.Tags{"landuse": "park", "layer": "2", "name": "foo"}),
	}
	filtered := SelectRelationPolygons(
		mapping.PolygonMatcher(),
		&r,
	)
	if len(filtered) != 3 {
		t.Fatal(filtered)
	}
	if filtered[0].Id != 0 || filtered[1].Id != 2 || filtered[2].Id != 4 {
		t.Fatal(filtered)
	}
}

func TestSelectRelationPolygonsMultipleTags(t *testing.T) {
	mapping, err := NewMapping("test_mapping.json")
	if err != nil {
		t.Fatal(err)
	}
	r := element.Relation{}
	r.Tags = element.Tags{"landuse": "forest", "natural": "scrub"}
	r.Members = []element.Member{
		makeMember(0, element.Tags{"natural": "scrub"}),
		makeMember(1, element.Tags{"landuse": "forest"}),
	}
	filtered := SelectRelationPolygons(
		mapping.PolygonMatcher(),
		&r,
	)
	// TODO both should be filterd out, but we only get one,
	// because we match only one tag per table
	if len(filtered) != 1 {
		t.Fatal(filtered)
	}
}

func TestSelectRelationPolygonsMultipleTagsOnWay(t *testing.T) {
	mapping, err := NewMapping("test_mapping.json")
	if err != nil {
		t.Fatal(err)
	}
	r := element.Relation{}
	r.Tags = element.Tags{"waterway": "riverbank"}
	r.Members = []element.Member{
		makeMemberRole(0, element.Tags{"waterway": "riverbank", "natural": "water"}, "outer"),
		makeMemberRole(1, element.Tags{"natural": "water"}, "inner"),
		makeMemberRole(2, element.Tags{"place": "islet"}, "inner"),
	}
	filtered := SelectRelationPolygons(
		mapping.PolygonMatcher(),
		&r,
	)

	if len(filtered) != 1 {
		t.Fatal(filtered)
	}
	if filtered[0].Id != 0 {
		t.Fatal(filtered)
	}
}
