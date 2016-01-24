package element

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)

func init() {
	Meta.Init()
}

type Tags map[string]string

func (t *Tags) String() string {
	return fmt.Sprintf("%v", (map[string]string)(*t))
}

type OSMElem struct {
	Id   int64 `json:"-"`
	Tags Tags  `json:"tags,omitempty"`
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

// OSM Metadata Parsing

var Meta MetaAttributes

type Metavar struct {
	Parse         bool   // need to parse ?
	KeyName       string // the name of the new osm Key - contains the metadata
	NameCollision int64  // number of the KeyName collision ;  must be updated atomically.
}

type MetaAttributes struct {
	Parse bool // true if any variable need to parse  ( Version or Timestamp or Changeset or Uid or User

	Version   Metavar // The edit version of the OSM object.
	Timestamp Metavar // Time of the last modification of the OSM object.
	Changeset Metavar // The OSM changeset in which the object was created or updated.
	Uid       Metavar // The numeric OSM user id. of the user who last modified the object.
	User      Metavar // The display name of the OSM user who last modified the object. A user can change their display name
}

func (ma *MetaAttributes) SetParse() {
	ma.Parse = ma.Version.Parse || ma.Timestamp.Parse || ma.Changeset.Parse || ma.Uid.Parse || ma.User.Parse
}

func (ma *MetaAttributes) Init() {
	ma.Version = Metavar{false, "_version_", 0}
	ma.Timestamp = Metavar{false, "_timestamp_", 0}
	ma.Changeset = Metavar{false, "_changeset_", 0}
	ma.Uid = Metavar{false, "_uid_", 0}
	ma.User = Metavar{false, "_user_", 0}

	ma.SetParse()
}

// Evaulate metadata keyname collision - and write a Warning
// Warning : metadata keyname collision  ....
// for example
//  _timestamp_ : http://taginfo.openstreetmap.org/keys/_timestamp_  ( Number of the objects : 17   ; data from2015-dec-14 )
//  _user_      : http://taginfo.openstreetmap.org/keys/_user_       ( Number of the objects : 17   ; data from2015-dec-14 )
func (mv Metavar) CollisionLog() {
	collision := atomic.LoadInt64(&mv.NameCollision)
	if collision > 0 {
		fmt.Println("Warning : metadata keyname collision ", mv.KeyName, "  : ", collision)
	}
}

func (ma *MetaAttributes) WriteCollisionLog() {
	ma.Version.CollisionLog()
	ma.Timestamp.CollisionLog()
	ma.Changeset.CollisionLog()
	ma.Uid.CollisionLog()
	ma.User.CollisionLog()
}

// don't add nodes/ways/relations with only "created_by" tag to nodes cache  = FALSE
var ParseDontAddOnlyCreatedByTag = true

// For storing OSM metadata
type MetaInfo struct {
	Version   int32
	Timestamp time.Time
	Changeset int64
	Uid       int32
	User      string
}

// Reset OSM MetaInfo to zero and empty string
func (metaInfo MetaInfo) Reset() {
	metaInfo.Version = 0
	metaInfo.Timestamp = time.Unix(0, 0)
	metaInfo.Changeset = 0
	metaInfo.Uid = 0
	metaInfo.User = ""
}

func (mv *Metavar) AddMetaKey(t Tags, keyvalue string) {

	if mv.Parse {

		// count key name collision
		if _, ok := t[mv.KeyName]; ok {
			atomic.AddInt64(&mv.NameCollision, 1)
		}

		// add new  key - value to OSM tags
		t[mv.KeyName] = keyvalue
	}

}

func (mv *Metavar) SetMetaKey(mappingKeyName string) {
	if mappingKeyName != "" {
		mv.Parse = true
		mv.KeyName = mappingKeyName
	}
}

// add OSM metadata to the Tags   and count key name collision .
func (t Tags) AddMetaInfo(info MetaInfo) {

	Meta.Version.AddMetaKey(t, strconv.FormatInt(int64(info.Version), 10))
	Meta.Timestamp.AddMetaKey(t, info.Timestamp.UTC().Format(time.RFC3339))
	Meta.Changeset.AddMetaKey(t, strconv.FormatInt(info.Changeset, 10))
	Meta.Uid.AddMetaKey(t, strconv.FormatInt(int64(info.Uid), 10))
	Meta.User.AddMetaKey(t, info.User)
}
