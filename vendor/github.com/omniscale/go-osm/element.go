package osm

import (
	"fmt"
	"time"
)

// A Tags is a collection of key=values, describing the OSM element.
type Tags map[string]string

func (t *Tags) String() string {
	return fmt.Sprintf("%v", (map[string]string)(*t))
}

// An Element contains information for nodes, ways and relations.
type Element struct {
	ID       int64
	Tags     Tags
	Metadata *Metadata
}

// A Matadata contains the optional metadata for each element.
type Metadata struct {
	UserID    int32
	UserName  string
	Version   int32
	Timestamp time.Time
	Changeset int64
}

// A Node contains lat/long coordinates.
type Node struct {
	Element
	Lat  float64
	Long float64
}

// A Way references one or more nodes by IDs.
type Way struct {
	Element
	// Refs specifies an ordered list of all node IDs that define this way.
	Refs []int64
	// Nodes specifies an ordered list of the actual nodes. Nodes can be empty
	// if the information is not available (e.g. during parsing).
	Nodes []Node
}

// IsClosed returns whether the first and last nodes are the same.
func (w *Way) IsClosed() bool {
	return len(w.Refs) >= 4 && w.Refs[0] == w.Refs[len(w.Refs)-1]
}

type MemberType int

const (
	NodeMember     MemberType = 0
	WayMember                 = 1
	RelationMember            = 2
)

// A Relation is a collection of multiple members.
type Relation struct {
	Element
	Members []Member
}

// A Member contains information about a single relation member.
type Member struct {
	// ID specifies the OpenStreetMap ID of the member. Note that nodes, ways
	// and relations each have their own range of IDs and the IDs are only uniq
	// within their type.
	ID int64
	// Type defines if the member is a Node, Way or Relation.
	Type MemberType
	// Role of the member. Strings like "inner", "outer", "stop", "platform", etc.
	// Interpretation of the role depends on the relation type.
	Role string
	// Way points to the actual Way, if Type is Way.
	// Can be nil if the information is not available (e.g. during parsing).
	Way *Way
	// Node points to the actual Node, if Type is NodeMember.
	// Can be nil if the information is not available (e.g. during parsing).
	Node *Node
	// Element points to the base information valid for all member types.
	// Can be nil if the information is not available (e.g. during parsing).
	Element *Element
}
