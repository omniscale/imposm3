package element

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)

type Tags map[string]string

func (t *Tags) String() string {
	return fmt.Sprintf("%v", (map[string]string)(*t))
}

// individual metadata parsing
var ParseMetadataVarVersion = false
var ParseMetadataVarTimestamp = false
var ParseMetadataVarChangeset = false
var ParseMetadataVarUid = false
var ParseMetadataVarUser = false

var ParseMetadataKeynameVersion = "_version_"
var ParseMetadataKeynameTimestamp = "_timestamp_"
var ParseMetadataKeynameChangeset = "_changeset_"
var ParseMetadataKeynameUid = "_uid_"
var ParseMetadataKeynameUser = "_user_"

// if any ParseMetadaVar* is 'true' ->  set ParseMetadata = true
var ParseMetadata = ParseMetadataVarVersion || ParseMetadataVarTimestamp || ParseMetadataVarChangeset || ParseMetadataVarUid || ParseMetadataVarUser

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

var countMetaVersion int64 = 0
var countMetaTimestamp int64 = 0
var countMetaChangeset int64 = 0
var countMetaUid int64 = 0
var countMetaUser int64 = 0

// Reset OSM MetaInfo to zero and empty string
func (metaInfo MetaInfo) Reset() {
	metaInfo.Version = 0
	metaInfo.Timestamp = time.Unix(0, 0)
	metaInfo.Changeset = 0
	metaInfo.Uid = 0
	metaInfo.User = ""
}

// Evaulate metadata keyname collision - and write a Warning
// Warning : metadata keyname collision  ....
// for example
//  _timestamp_ : http://taginfo.openstreetmap.org/keys/_timestamp_  ( Number of the objects : 17   ; data from2015-dec-14 )
//  _user_      : http://taginfo.openstreetmap.org/keys/_user_       ( Number of the objects : 17   ; data from2015-dec-14 )
//
func WriteMetaInfo() {

	mcountMetaVersion := atomic.LoadInt64(&countMetaVersion)
	if mcountMetaVersion > 0 {
		fmt.Println("Warning : metadata keyname collision ", ParseMetadataKeynameVersion, "  : ", mcountMetaVersion)
	}

	mcountMetaTimestamp := atomic.LoadInt64(&countMetaTimestamp)
	if mcountMetaTimestamp > 0 {
		fmt.Println("Warning : metadata keyname collision ", ParseMetadataKeynameTimestamp, "  : ", mcountMetaTimestamp)
	}

	mcountMetaChangeset := atomic.LoadInt64(&countMetaChangeset)
	if mcountMetaChangeset > 0 {
		fmt.Println("Warning : metadata keyname collision ", ParseMetadataKeynameChangeset, "  : ", mcountMetaChangeset)
	}

	mcountMetaUid := atomic.LoadInt64(&countMetaUid)
	if mcountMetaUid > 0 {
		fmt.Println("Warning : metadata keyname collision ", ParseMetadataKeynameUid, "  : ", mcountMetaUid)
	}

	mcountMetaUser := atomic.LoadInt64(&countMetaUser)
	if mcountMetaUser > 0 {
		fmt.Println("Warning : metadata keyname collision ", ParseMetadataKeynameUser, "  : ", mcountMetaUser)
	}

}

// add OSM metadata to the Tags   and count key name collision .
func (t Tags) AddMetaInfo(info MetaInfo) {

	if ParseMetadataVarVersion {
		if _, ok := t[ParseMetadataKeynameVersion]; ok {

			atomic.AddInt64(&countMetaVersion, 1)
		}
		t[ParseMetadataKeynameVersion] = strconv.FormatInt(int64(info.Version), 10)
	}

	if ParseMetadataVarTimestamp {
		if _, ok := t[ParseMetadataKeynameTimestamp]; ok {
			// count key name collision
			atomic.AddInt64(&countMetaTimestamp, 1)
		}
		t[ParseMetadataKeynameTimestamp] = info.Timestamp.UTC().Format(time.RFC3339)
	}

	if ParseMetadataVarChangeset {
		if _, ok := t[ParseMetadataKeynameChangeset]; ok {
			// count key name collision
			atomic.AddInt64(&countMetaChangeset, 1)
		}
		t[ParseMetadataKeynameChangeset] = strconv.FormatInt(info.Changeset, 10)
	}

	if ParseMetadataVarUid {
		if _, ok := t[ParseMetadataKeynameUid]; ok {
			// count key name collision
			atomic.AddInt64(&countMetaUid, 1)
		}
		t[ParseMetadataKeynameUid] = strconv.FormatInt(int64(info.Uid), 10)
	}

	if ParseMetadataVarUser {
		if _, ok := t[ParseMetadataKeynameUser]; ok {
			// count key name collision
			atomic.AddInt64(&countMetaUser, 1)
		}
		t[ParseMetadataKeynameUser] = info.User
	}

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
