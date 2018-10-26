package binary

import (
	"testing"

	osm "github.com/omniscale/go-osm"
)

func compareRefs(a []int64, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func TestMarshalNode(t *testing.T) {
	node := &osm.Node{}
	node.ID = 12345
	node.Tags = make(osm.Tags)
	node.Tags["name"] = "test"
	node.Tags["place"] = "city"

	data, _ := MarshalNode(node)
	node, _ = UnmarshalNode(data)

	if node.Tags["name"] != "test" {
		t.Error("name tag does not match")
	}
	if node.Tags["place"] != "city" {
		t.Error("place tag does not match")
	}

	if len(node.Tags) != 2 {
		t.Error("tags len does not match")
	}

}

func TestMarshalWay(t *testing.T) {
	way := &osm.Way{}
	way.ID = 12345
	way.Tags = make(osm.Tags)
	way.Tags["name"] = "test"
	way.Tags["highway"] = "trunk"
	way.Refs = append(way.Refs, 1, 2, 3, 4)

	data, _ := MarshalWay(way)
	way, _ = UnmarshalWay(data)

	if way.Tags["name"] != "test" {
		t.Error("name tag does not match")
	}
	if way.Tags["highway"] != "trunk" {
		t.Error("highway tag does not match")
	}

	if len(way.Tags) != 2 {
		t.Error("tags len does not match")
	}

	if !compareRefs(way.Refs, []int64{1, 2, 3, 4}) {
		t.Error("nodes do not match")
	}

}

func BenchmarkMarshalWay(b *testing.B) {
	b.ReportAllocs()
	way := &osm.Way{}
	way.ID = 12345
	way.Tags = make(osm.Tags)
	way.Tags["name"] = "test"
	way.Tags["highway"] = "trunk"
	way.Refs = append(way.Refs, 1, 2, 3, 4)

	for i := 0; i < b.N; i++ {
		_, _ = MarshalWay(way)
	}
}

func BenchmarkUnmarshalWay(b *testing.B) {
	b.ReportAllocs()
	way := &osm.Way{}
	way.ID = 12345
	way.Tags = make(osm.Tags)
	way.Tags["name"] = "test"
	way.Tags["highway"] = "trunk"
	way.Refs = append(way.Refs, 1, 2, 3, 4)

	data, _ := MarshalWay(way)
	for i := 0; i < b.N; i++ {
		_, _ = UnmarshalWay(data)
	}
}

func TestMarshalRelation(t *testing.T) {
	rel := &osm.Relation{}
	rel.ID = 12345
	rel.Tags = make(osm.Tags)
	rel.Tags["name"] = "test"
	rel.Tags["landusage"] = "forest"
	rel.Members = append(rel.Members, osm.Member{ID: 123, Type: osm.WAY, Role: "outer"})
	rel.Members = append(rel.Members, osm.Member{ID: 124, Type: osm.WAY, Role: "inner"})

	data, _ := MarshalRelation(rel)
	rel, _ = UnmarshalRelation(data)

	if rel.Tags["name"] != "test" {
		t.Error("name tag does not match")
	}
	if rel.Tags["landusage"] != "forest" {
		t.Error("landusage tag does not match")
	}

	if len(rel.Tags) != 2 {
		t.Error("tags len does not match")
	}

	if len(rel.Members) != 2 {
		t.Error("members len does not match")
	}

	if rel.Members[0].ID != 123 || rel.Members[0].Type != osm.WAY || rel.Members[0].Role != "outer" {
		t.Error("members do not match", rel.Members[0])
	}

	if rel.Members[1].ID != 124 || rel.Members[1].Type != osm.WAY || rel.Members[1].Role != "inner" {
		t.Error("members do not match", rel.Members[1])
	}
}

func TestDeltaPack(t *testing.T) {
	ids := []int64{1000, 999, 1001, -8, 1234}
	deltaPack(ids)

	for i, id := range []int64{1000, -1, 2, -1009, 1242} {
		if ids[i] != id {
			t.Fatal(ids[i], id, ids)
		}
	}
}

func TestDeltaUnpack(t *testing.T) {
	ids := []int64{1000, -1, 2, -1009, 1242}
	deltaUnpack(ids)

	for i, id := range []int64{1000, 999, 1001, -8, 1234} {
		if ids[i] != id {
			t.Fatal(ids[i], id, ids)
		}
	}
}
