package geom

import (
	"imposm3/element"
	"sort"
	"testing"
)

func TestRingMerge(t *testing.T) {
	w1 := element.Way{}
	w1.Id = 1
	w1.Refs = []int64{1, 2, 3}
	w1.Nodes = []element.Node{
		element.Node{},
		element.Node{},
		element.Node{},
	}
	r1 := NewRing(&w1)

	w2 := element.Way{}
	w2.Id = 2
	w2.Refs = []int64{3, 4, 1}
	w2.Nodes = []element.Node{
		element.Node{},
		element.Node{},
		element.Node{},
	}
	r2 := NewRing(&w2)
	rings := []*Ring{r1, r2}

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

func TestRingMergeReverseEndpoints(t *testing.T) {
	w1 := element.Way{}
	w1.Id = 1
	w1.Refs = []int64{1, 2, 3, 4}
	w1.Nodes = []element.Node{
		element.Node{},
		element.Node{},
		element.Node{},
		element.Node{},
	}
	r1 := NewRing(&w1)

	w2 := element.Way{}
	w2.Id = 2
	w2.Refs = []int64{6, 5, 4}
	w2.Nodes = []element.Node{
		element.Node{},
		element.Node{},
		element.Node{},
	}
	r2 := NewRing(&w2)

	w3 := element.Way{}
	w3.Id = 3
	w3.Refs = []int64{1, 7, 6}
	w3.Nodes = []element.Node{
		element.Node{},
		element.Node{},
		element.Node{},
	}
	r3 := NewRing(&w3)

	rings := []*Ring{r1, r2, r3}

	result := mergeRings(rings)
	if len(result) != 1 {
		t.Fatal(result)
	}
	r := result[0]
	expected := []int64{6, 5, 4, 3, 2, 1, 7, 6}
	for i, ref := range r.refs {
		if ref != expected[i] {
			t.Fatalf("%v != %v", r.refs, expected)
		}
	}
}

func TestRingMergePermutations(t *testing.T) {
	// Test all possible permutations of 4 ring segments.
	for i := 0; i < 16; i++ {
		// test each segment in both directions
		f1 := i&1 == 0
		f2 := i&2 == 0
		f3 := i&4 == 0
		f4 := i&8 == 0

		indices := []int{0, 1, 2, 3}

		for permutationFirst(sort.IntSlice(indices)); permutationNext(sort.IntSlice(indices)); {
			ways := make([][]int64, 4)
			if f1 {
				ways[0] = []int64{1, 2, 3, 4}
			} else {
				ways[0] = []int64{4, 3, 2, 1}
			}
			if f2 {
				ways[1] = []int64{4, 5, 6, 7}
			} else {
				ways[1] = []int64{7, 6, 5, 4}
			}
			if f3 {
				ways[2] = []int64{7, 8, 9, 10}
			} else {
				ways[2] = []int64{10, 9, 8, 7}
			}
			if f4 {
				ways[3] = []int64{10, 11, 12, 1}
			} else {
				ways[3] = []int64{1, 12, 11, 10}
			}

			w1 := element.Way{}
			w1.Id = 1
			w1.Refs = ways[indices[0]]
			w1.Nodes = []element.Node{element.Node{}, element.Node{}, element.Node{}, element.Node{}}
			w2 := element.Way{}
			w2.Id = 2
			w2.Refs = ways[indices[1]]
			w2.Nodes = []element.Node{element.Node{}, element.Node{}, element.Node{}, element.Node{}}
			w3 := element.Way{}
			w3.Id = 3
			w3.Refs = ways[indices[2]]
			w3.Nodes = []element.Node{element.Node{}, element.Node{}, element.Node{}, element.Node{}}
			w4 := element.Way{}
			w4.Id = 4
			w4.Refs = ways[indices[3]]
			w4.Nodes = []element.Node{element.Node{}, element.Node{}, element.Node{}, element.Node{}}

			rings := []*Ring{
				&Ring{ways: []*element.Way{&w1}, refs: w1.Refs, nodes: w1.Nodes},
				&Ring{ways: []*element.Way{&w2}, refs: w2.Refs, nodes: w2.Nodes},
				&Ring{ways: []*element.Way{&w3}, refs: w3.Refs, nodes: w3.Nodes},
				&Ring{ways: []*element.Way{&w4}, refs: w4.Refs, nodes: w4.Nodes},
			}
			result := mergeRings(rings)
			if len(result) != 1 {
				t.Fatalf("not a single ring: %v\n", result)
			}

			r := result[0].refs

			if r[0] != r[len(r)-1] {
				t.Fatalf("ring not closed: %v", r)
			}

			asc := true
			desc := true

			for i := 1; i < len(r); i++ {
				if r[i] == 1 || r[i-1] < r[i] {
					continue
				} else {
					asc = false
					break
				}
			}
			for i := 1; i < len(r); i++ {
				if r[i] == 12 || r[i-1] > r[i] {
					continue
				} else {
					desc = false
					break
				}
			}

			if !(asc || desc) {
				t.Fatalf("ring not ascending/descending: %v, asc: %v, desc: %v", r, asc, desc)
			}
		}
	}
}
