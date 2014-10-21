package geom

import (
	"math"

	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/geom/geos"
)

type Ring struct {
	ways        []*element.Way
	refs        []int64
	nodes       []element.Node
	geom        *geos.Geom
	holes       map[*Ring]bool
	containedBy int
	area        float64
	inserted    map[int64]bool
}

func (r *Ring) IsClosed() bool {
	return len(r.refs) >= 4 && r.refs[0] == r.refs[len(r.refs)-1]
}

// TryClose closes the ring if both end nodes are nearly identical.
// Returns true if it succeeds.
func (r *Ring) TryClose(maxRingGap float64) bool {
	if len(r.refs) < 4 {
		return false
	}
	start, end := r.nodes[0], r.nodes[len(r.nodes)-1]
	dist := math.Hypot(start.Lat-end.Lat, start.Long-end.Long)
	if dist < maxRingGap {
		r.refs[len(r.refs)-1] = r.refs[0]
		r.nodes[len(r.nodes)-1] = r.nodes[0]
		return true
	}
	return false
}

func (r *Ring) MarkInserted(tags element.Tags) {
	if r.inserted == nil {
		r.inserted = make(map[int64]bool)
	}
	for _, w := range r.ways {
		if tagsSameOrEmpty(tags, w.Tags) {
			r.inserted[w.Id] = true
		}
	}
}

func tagsSameOrEmpty(a, b element.Tags) bool {
	if len(b) == 0 {
		return true
	}
	for k, v := range a {
		if k == "name" {
			continue
		}
		if cmp, ok := b[k]; !ok || v != cmp {
			return false
		}
	}
	for k, v := range b {
		if k == "name" {
			continue
		}
		if cmp, ok := a[k]; !ok || v != cmp {
			return false
		}
	}
	return true
}

func NewRing(way *element.Way) *Ring {
	ring := Ring{}
	ring.ways = []*element.Way{way}
	ring.refs = make([]int64, len(way.Refs))
	ring.nodes = make([]element.Node, len(way.Nodes))
	ring.containedBy = -1
	ring.holes = make(map[*Ring]bool)
	copy(ring.refs, way.Refs)
	copy(ring.nodes, way.Nodes)
	return &ring
}

func reverseRefs(refs []int64) {
	for i, j := 0, len(refs)-1; i < j; i, j = i+1, j-1 {
		refs[i], refs[j] = refs[j], refs[i]
	}
}

func reverseNodes(nodes []element.Node) {
	for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}
}

func mergeRings(rings []*Ring) []*Ring {
	endpoints := make(map[int64]*Ring)

	for _, ring := range rings {
		if len(ring.refs) < 2 {
			continue
		}
		left := ring.refs[0]
		right := ring.refs[len(ring.refs)-1]

		if origRing, ok := endpoints[left]; ok {
			// left node connects to..
			delete(endpoints, left)
			if left == origRing.refs[len(origRing.refs)-1] {
				// .. right end
				origRing.refs = append(origRing.refs, ring.refs[1:]...)
				origRing.nodes = append(origRing.nodes, ring.nodes[1:]...)
			} else {
				// .. left end, reverse ring
				reverseRefs(origRing.refs)
				origRing.refs = append(origRing.refs, ring.refs[1:]...)
				reverseNodes(origRing.nodes)
				origRing.nodes = append(origRing.nodes, ring.nodes[1:]...)
			}
			origRing.ways = append(origRing.ways, ring.ways...)
			if rightRing, ok := endpoints[right]; ok && rightRing != origRing {
				// right node connects to another ring, close ring
				delete(endpoints, right)
				if right == rightRing.refs[0] {
					origRing.refs = append(origRing.refs, rightRing.refs[1:]...)
					origRing.nodes = append(origRing.nodes, rightRing.nodes[1:]...)
				} else {
					reverseRefs(rightRing.refs)
					origRing.refs = append(origRing.refs[:len(origRing.refs)-1], rightRing.refs...)
					reverseNodes(rightRing.nodes)
					origRing.nodes = append(origRing.nodes[:len(origRing.nodes)-1], rightRing.nodes...)
				}
				origRing.ways = append(origRing.ways, rightRing.ways...)
				right := origRing.refs[len(origRing.refs)-1]
				endpoints[right] = origRing
			} else {
				endpoints[right] = origRing
			}
		} else if origRing, ok := endpoints[right]; ok {
			// right node connects to..
			delete(endpoints, right)
			if right == origRing.refs[0] {
				// .. left end
				origRing.refs = append(ring.refs[:len(ring.refs)-1], origRing.refs...)
				origRing.nodes = append(ring.nodes[:len(ring.nodes)-1], origRing.nodes...)
			} else {
				// .. right end, reverse ring
				reverseRefs(ring.refs)
				origRing.refs = append(origRing.refs[:len(origRing.refs)-1], ring.refs...)
				reverseNodes(ring.nodes)
				origRing.nodes = append(origRing.nodes[:len(origRing.nodes)-1], ring.nodes...)
			}
			origRing.ways = append(origRing.ways, ring.ways...)
			endpoints[left] = origRing
		} else {
			// ring is not connected (yet)
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
