package geom

import (
	"goposm/element"
)

type Ring struct {
	ways  []*element.Way
	refs  []int64
	nodes []*element.Node
}

func reverseRefs(refs []int64) {
	for i, j := 0, len(refs)-1; i < j; i, j = i+1, j-1 {
		refs[i], refs[j] = refs[j], refs[i]
	}
}

func reverseNodes(nodes []*element.Node) {
	for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}
}

func mergeRings(rings []*Ring) []*Ring {
	endpoints := make(map[int64]*Ring)
	for _, ring := range rings {
		left := ring.refs[0]
		right := ring.refs[len(ring.refs)-1]
		if origRing, ok := endpoints[left]; ok {
			delete(endpoints, left)
			if left == origRing.refs[len(origRing.refs)-1] {
				origRing.refs = append(origRing.refs, ring.refs[1:]...)
				origRing.nodes = append(origRing.nodes, ring.nodes[1:]...)
			} else {
				reverseRefs(origRing.refs)
				origRing.refs = append(origRing.refs, ring.refs[1:]...)
				reverseNodes(origRing.nodes)
				origRing.nodes = append(origRing.nodes, ring.nodes[1:]...)
			}
			origRing.ways = append(origRing.ways, ring.ways...)
			// TODO tags

			if rightRing, ok := endpoints[right]; ok && rightRing != origRing {
				// close gap
				delete(endpoints, right)
				if right == ring.refs[0] {
					origRing.refs = append(origRing.refs, ring.refs[1:]...)
					origRing.nodes = append(origRing.nodes, ring.nodes[1:]...)
				} else {

					reverseRefs(ring.refs)
					origRing.refs = append(origRing.refs[:len(origRing.refs)-2], ring.refs...)
					reverseNodes(ring.nodes)
					origRing.nodes = append(origRing.nodes[:len(origRing.nodes)-2], ring.nodes...)
				}

				origRing.ways = append(origRing.ways, ring.ways...)
				right := origRing.refs[len(origRing.refs)-1]
				endpoints[right] = origRing

			} else {
				endpoints[right] = origRing
			}
		} else if origRing, ok := endpoints[right]; ok {
			delete(endpoints, right)
			if right == origRing.refs[0] {
				origRing.refs = append(ring.refs[:len(ring.refs)-1], origRing.refs...)
				origRing.nodes = append(ring.nodes[:len(ring.nodes)-1], origRing.nodes...)
			} else {
				reverseRefs(ring.refs)
				origRing.refs = append(origRing.refs[:len(origRing.refs)-2], ring.refs...)
				reverseNodes(ring.nodes)
				origRing.nodes = append(origRing.nodes[:len(origRing.nodes)-2], ring.nodes...)
			}
			origRing.ways = append(origRing.ways, ring.ways...)
			// TODO tags
			endpoints[left] = origRing
		} else {
			endpoints[left] = ring
			endpoints[right] = ring
		}
	}
	uniqueRings := make(map[*Ring]bool)
	for _, ring := range endpoints {
		uniqueRings[ring] = true
	}
	result := make([]*Ring, 0, len(uniqueRings))
	for ring, _ := range uniqueRings {
		result = append(result, ring)
	}
	return result
}
