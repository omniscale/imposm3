package element

type Tags map[string]string

type OSMElem struct {
	Id   int64
	Tags Tags
}

type Node struct {
	Id   int64
	Tags Tags
	Lat  float64
	Long float64
}

type Way struct {
	Id    int64
	Tags  Tags
	Nodes []int64
}

type MemberType int

const (
	NODE MemberType = iota
	WAY
	RELATION
)

type Member struct {
	Id   int64
	Type MemberType
	Role string
}

type Relation struct {
	Id      int64
	Tags    Tags
	Members []Member
}
