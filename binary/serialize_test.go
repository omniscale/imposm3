package binary

import (
	"goposm/element"
	"testing"
)

func compareNodes(a []int64, b []int64) bool {
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
	node := &element.Node{}
	node.Id = 12345
	node.Tags = make(element.Tags)
	node.Tags["name"] = "test"
	node.Tags["place"] = "city"

	data, _ := MarshalNode(node)
	node, _ = UnmarshalNode(data)
	if node.Id != 12345 {
		t.Error("id does not match")
	}

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
	way := &element.Way{}
	way.Id = 12345
	way.Tags = make(element.Tags)
	way.Tags["name"] = "test"
	way.Tags["highway"] = "trunk"
	way.Refs = append(way.Refs, 1, 2, 3, 4)

	data, _ := MarshalWay(way)
	way, _ = UnmarshalWay(data)
	if way.Id != 12345 {
		t.Error("id does not match")
	}

	if way.Tags["name"] != "test" {
		t.Error("name tag does not match")
	}
	if way.Tags["highway"] != "trunk" {
		t.Error("highway tag does not match")
	}

	if len(way.Tags) != 2 {
		t.Error("tags len does not match")
	}

	if !compareNodes(way.Refs, []int64{1, 2, 3, 4}) {
		t.Error("nodes do not match")
	}

}
