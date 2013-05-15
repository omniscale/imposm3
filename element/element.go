package element

import (
	"goposm/geom/geos"
)

type Tags map[string]string

type OSMElem struct {
	Id   int64
	Tags Tags
	Geom *Geometry
}

type Node struct {
	OSMElem
	Lat  float64
	Long float64
}

type Way struct {
	OSMElem
	Refs  []int64
	Nodes []Node
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

type Member struct {
	Id   int64
	Type MemberType
	Role string
}

type Relation struct {
	OSMElem
	Members []Member
}

func (elem *OSMElem) TagsFromArray(arr []string) {
	result := make(Tags)
	for i := 0; i < len(arr); i += 2 {
		result[arr[i]] = arr[i+1]
	}
	elem.Tags = result
}

func (elem *OSMElem) TagsAsArray() []string {
	result := make([]string, 0, 2*len(elem.Tags))
	for key, val := range elem.Tags {
		result = append(result, key, val)
	}
	return result
}
