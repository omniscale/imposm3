package element

import (
	"sort"

	"imposm3/geom/geos"
)

type Tags map[string]string

type OSMElem struct {
	Id   int64     `json:"-"`
	Tags Tags      `json:"tags,omitempty"`
	Geom *Geometry `json:"-"`
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

type Geometry struct {
	Geom *geos.Geom
	Wkb  []byte
}

func (w *Way) IsClosed() bool {
	return len(w.Refs) >= 4 && w.Refs[0] == w.Refs[len(w.Refs)-1]
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
}

type Relation struct {
	OSMElem
	Members []Member `json:"members"`
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
