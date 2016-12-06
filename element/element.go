package element

import (
	"fmt"
	"math"
	"sort"
	"time"
)

type Tags map[string]string

func (t *Tags) String() string {
	return fmt.Sprintf("%v", (map[string]string)(*t))
}

type OSMElem struct {
	Id       int64     `json:"-"`
	Tags     Tags      `json:"tags,omitempty"`
	Metadata *Metadata `json:"-"`
}

type Node struct {
	OSMElem
	Lat  float64 `json:"lat"`
	Long float64 `json:"lon"`
}

type Way struct {
	OSMElem
	Refs  []int64 `json:"refs"`
	Nodes []Node  `json:"nodes,omitempty"`
}

func (w *Way) IsClosed() bool {
	return len(w.Refs) >= 4 && w.Refs[0] == w.Refs[len(w.Refs)-1]
}

func (w *Way) TryClose(maxGap float64) bool {
	return TryCloseWay(w.Refs, w.Nodes, maxGap)
}

// TryCloseWay closes the way if both end nodes are nearly identical.
// Returns true if it succeeds.
func TryCloseWay(refs []int64, nodes []Node, maxGap float64) bool {
	if len(refs) < 4 {
		return false
	}
	start, end := nodes[0], nodes[len(nodes)-1]
	dist := math.Hypot(start.Lat-end.Lat, start.Long-end.Long)
	if dist < maxGap {
		refs[len(refs)-1] = refs[0]
		nodes[len(nodes)-1] = nodes[0]
		return true
	}
	return false
}

type MemberType int

const (
	NODE     MemberType = 0
	WAY                 = 1
	RELATION            = 2
)

var MemberTypeValues = map[string]MemberType{
	"node":     NODE,
	"way":      WAY,
	"relation": RELATION,
}

type Member struct {
	Id   int64      `json:"id"`
	Type MemberType `json:"type"`
	Role string     `json:"role"`
	Way  *Way       `json:"-"`
	Node *Node      `json:"-"`
	Elem *OSMElem   `json:"-"`
}

type Relation struct {
	OSMElem
	Members []Member `json:"members"`
}

type Metadata struct {
	UserId    int
	UserName  string
	Version   int
	Timestamp time.Time
	Changeset int
}

type IdRefs struct {
	Id   int64
	Refs []int64
}

func (idRefs *IdRefs) Add(ref int64) {
	i := sort.Search(len(idRefs.Refs), func(i int) bool {
		return idRefs.Refs[i] >= ref
	})
	if i < len(idRefs.Refs) && idRefs.Refs[i] >= ref {
		if idRefs.Refs[i] > ref {
			idRefs.Refs = append(idRefs.Refs, 0)
			copy(idRefs.Refs[i+1:], idRefs.Refs[i:])
			idRefs.Refs[i] = ref
		} // else already inserted
	} else {
		idRefs.Refs = append(idRefs.Refs, ref)
	}
}

func (idRefs *IdRefs) Delete(ref int64) {
	i := sort.Search(len(idRefs.Refs), func(i int) bool {
		return idRefs.Refs[i] >= ref
	})
	if i < len(idRefs.Refs) && idRefs.Refs[i] == ref {
		idRefs.Refs = append(idRefs.Refs[:i], idRefs.Refs[i+1:]...)
	}
}

// RelIdOffset is a constant we subtract from relation IDs
// to avoid conflicts with way and node IDs.
// Nodes, ways and relations have separate ID spaces in OSM, but
// we need unique IDs for updating and removing elements in diff mode.
// In a normal diff import relation IDs are negated to distinguish them
// from way IDs, because ways and relations can both be imported in the
// same polygon table.
// Nodes are only imported together with ways and relations in single table
// imports (see `type_mappings`). In this case we negate the way and
// relation IDs and aditionaly subtract RelIdOffset from the relation IDs.
// Ways will go from -0 to -100,000,000,000,000,000, relations from
// -100,000,000,000,000,000 down wards.
const RelIdOffset = -1e17
