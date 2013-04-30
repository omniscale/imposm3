package geom

import (
	"goposm/element"
	"testing"
)

func TestRingMerge(t *testing.T) {
	w1 := element.Way{}
	w1.Id = 1
	w1.Refs = []int64{1, 2, 3}
	nodes := []*element.Node{
		&element.Node{},
		&element.Node{},
		&element.Node{},
	}
	r1 := Ring{[]*element.Way{&w1}, w1.Refs, nodes}

	w2 := element.Way{}
	w2.Id = 2
	w2.Refs = []int64{3, 4, 1}
	nodes = []*element.Node{
		&element.Node{},
		&element.Node{},
		&element.Node{},
	}
	r2 := Ring{[]*element.Way{&w2}, w2.Refs, nodes}
	rings := []*Ring{&r1, &r2}

	result := mergeRings(rings)
	if len(result) != 1 {
		t.Fatal(result)
	}
	r := result[0]
	expected := []int64{1, 2, 3, 4, 1}
	for i, ref := range r.refs {
		if ref != expected[i] {
			t.Fatalf("%v != %v", r.refs, expected)
		}
	}
}
