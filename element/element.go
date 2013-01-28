package element

type Tags map[string]string
type OSMID int64

type OSMElem struct {
	Id   OSMID
	Tags Tags
}

type Node struct {
	OSMElem
	Lat  float64
	Long float64
}

type Way struct {
	OSMElem
	Nodes []OSMID
}

type MemberType int

const (
	NODE     MemberType = iota
	WAY      MemberType = iota
	RELATION MemberType = iota
)

type Member struct {
	Id   OSMID
	Type MemberType
	Role string
}

type Relation struct {
	OSMElem
	Members []Member
}
