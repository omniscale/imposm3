package pbf

import (
	"time"

	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/parser/pbf/osmpbf"
)

const coord_factor float64 = 11930464.7083 // ((2<<31)-1)/360.0

func coordToInt(coord float64) uint32 {
	return uint32((coord + 180.0) * coord_factor)
}

func intToCoord(coord uint32) float64 {
	return float64((float64(coord) / coord_factor) - 180.0)
}

func readDenseNodes(
	dense *osmpbf.DenseNodes,
	block *osmpbf.PrimitiveBlock,
	stringtable stringTable) (coords []element.Node, nodes []element.Node) {

	var lastId int64
	var lastLon, lastLat int64
	coords = make([]element.Node, len(dense.Id))
	nodes = make([]element.Node, 0, len(dense.Id)/8)
	granularity := int64(block.GetGranularity())
	latOffset := block.GetLatOffset()
	lonOffset := block.GetLonOffset()
	coordScale := 0.000000001
	lastKeyValPos := 0

	//if element.ParseMetadata {
	var metaInfo element.MetaInfo

	var lastDenseInfoTimestamp int64
	var lastDenseInfoChangeset int64
	var lastDenseInfoUid int32
	var lastDenseInfoUserSid int32

	var denseInfoVersion int32
	var denseInfoTimestamp time.Time
	var denseInfoChangeset int64
	var denseInfoUid int32
	var denseInfoUserSid int32
	//}    //endif element.ParseMetadata

	for i := range coords {
		lastId += dense.Id[i]
		lastLon += dense.Lon[i]
		lastLat += dense.Lat[i]

		coords[i].Id = lastId
		coords[i].Long = (coordScale * float64(lonOffset+(granularity*lastLon)))
		coords[i].Lat = (coordScale * float64(latOffset+(granularity*lastLat)))

		if element.Meta.Parse {
			lastDenseInfoTimestamp += dense.Denseinfo.Timestamp[i]
			lastDenseInfoChangeset += dense.Denseinfo.Changeset[i]
			lastDenseInfoUid += dense.Denseinfo.Uid[i]
			lastDenseInfoUserSid += dense.Denseinfo.UserSid[i]

			denseInfoVersion = dense.Denseinfo.Version[i]
			denseInfoTimestamp = time.Unix(lastDenseInfoTimestamp, 0)
			denseInfoChangeset = lastDenseInfoChangeset
			denseInfoUid = lastDenseInfoUid
			denseInfoUserSid = lastDenseInfoUserSid
		}

		if stringtable != nil && len(dense.KeysVals) > 0 {
			if dense.KeysVals[lastKeyValPos] != 0 {
				tags := parseDenseNodeTags(stringtable, &dense.KeysVals, &lastKeyValPos)

				if tags != nil {

					if _, ok := tags["created_by"]; ok && element.ParseDontAddOnlyCreatedByTag && (len(tags) == 1) {
						// don't add nodes with only created_by tag to nodes cache

					} else {

						if element.Meta.Parse {

							metaInfo.Version = denseInfoVersion
							metaInfo.Timestamp = denseInfoTimestamp
							metaInfo.Changeset = denseInfoChangeset
							metaInfo.Uid = denseInfoUid
							metaInfo.User = stringtable[denseInfoUserSid]

							tags.AddMetaInfo(metaInfo)
						}
						nd := coords[i]
						nd.Tags = tags
						nodes = append(nodes, nd)
					}
				}
			} else {
				lastKeyValPos += 1
			}
		}
	}

	return coords, nodes
}

func parseDenseNodeTags(stringtable stringTable, keysVals *[]int32, pos *int) element.Tags {
	// make map later if needed
	var result element.Tags
	for {
		if *pos >= len(*keysVals) {
			return result
		}
		key := (*keysVals)[*pos]
		*pos += 1
		if key == 0 {
			return result
		}
		val := (*keysVals)[*pos]
		*pos += 1
		if result == nil {
			result = make(element.Tags)
		}
		result[stringtable[key]] = stringtable[val]
	}
}

func parseTags(stringtable stringTable, keys []uint32, vals []uint32) element.Tags {
	if len(keys) == 0 {
		return nil
	}
	tags := make(element.Tags)
	for i := 0; i < len(keys); i++ {
		key := stringtable[keys[i]]
		val := stringtable[vals[i]]
		tags[key] = val
	}
	return tags
}

func readNodes(
	nodes []*osmpbf.Node,
	block *osmpbf.PrimitiveBlock,
	stringtable stringTable) ([]element.Node, []element.Node) {

	coords := make([]element.Node, len(nodes))
	nds := make([]element.Node, 0, len(nodes)/8)
	granularity := int64(block.GetGranularity())
	latOffset := block.GetLatOffset()
	lonOffset := block.GetLonOffset()
	coordScale := 0.000000001

	var metaInfo element.MetaInfo

	for i := range nodes {
		id := *nodes[i].Id
		lon := *nodes[i].Lon
		lat := *nodes[i].Lat
		coords[i].Id = id
		coords[i].Long = (coordScale * float64(lonOffset+(granularity*lon)))
		coords[i].Lat = (coordScale * float64(latOffset+(granularity*lat)))
		if stringtable != nil {
			tags := parseTags(stringtable, nodes[i].Keys, nodes[i].Vals)

			if tags != nil {
				if _, ok := tags["created_by"]; ok && element.ParseDontAddOnlyCreatedByTag && (len(tags) == 1) {
					// don't add nodes with only created_by tag to nodes cache
				} else {

					if element.Meta.Parse {

						metaInfo.Version = *nodes[i].Info.Version
						metaInfo.Timestamp = time.Unix(*nodes[i].Info.Timestamp, 0)
						metaInfo.Changeset = *nodes[i].Info.Changeset
						metaInfo.Uid = *nodes[i].Info.Uid
						metaInfo.User = stringtable[nodes[i].GetInfo().GetUserSid()]

						tags.AddMetaInfo(metaInfo)
					}

					nd := coords[i]
					nd.Tags = tags
					nds = append(nds, nd)
				}
			}
		}
	}
	return coords, nds
}

func parseDeltaRefs(refs []int64) []int64 {
	result := make([]int64, len(refs))
	var lastRef int64

	for i, refDelta := range refs {
		lastRef += refDelta
		result[i] = lastRef
	}
	return result
}

func readWays(
	ways []*osmpbf.Way,
	block *osmpbf.PrimitiveBlock,
	stringtable stringTable) []element.Way {

	result := make([]element.Way, len(ways))

	var metaInfo element.MetaInfo

	for i := range ways {
		id := *ways[i].Id
		result[i].Id = id
		result[i].Tags = parseTags(stringtable, ways[i].Keys, ways[i].Vals)
		result[i].Refs = parseDeltaRefs(ways[i].Refs)

		if element.Meta.Parse && (ways[i].Info != nil) && (len(result[i].Tags) > 0) {

			metaInfo.Version = *ways[i].Info.Version
			metaInfo.Timestamp = time.Unix(*ways[i].Info.Timestamp, 0)
			metaInfo.Changeset = *ways[i].Info.Changeset
			metaInfo.Uid = *ways[i].Info.Uid
			metaInfo.User = stringtable[ways[i].GetInfo().GetUserSid()]

			result[i].Tags.AddMetaInfo(metaInfo)
		}
	}
	return result
}

func parseRelationMembers(rel *osmpbf.Relation, stringtable stringTable) []element.Member {
	result := make([]element.Member, len(rel.Memids))

	var lastId int64
	for i := range rel.Memids {
		lastId += rel.Memids[i]
		result[i].Id = lastId
		result[i].Role = stringtable[rel.RolesSid[i]]
		result[i].Type = element.MemberType(rel.Types[i])
	}
	return result
}

func readRelations(
	relations []*osmpbf.Relation,
	block *osmpbf.PrimitiveBlock,
	stringtable stringTable) []element.Relation {

	result := make([]element.Relation, len(relations))
	var metaInfo element.MetaInfo

	for i := range relations {
		id := *relations[i].Id
		result[i].Id = id
		result[i].Tags = parseTags(stringtable, relations[i].Keys, relations[i].Vals)
		result[i].Members = parseRelationMembers(relations[i], stringtable)

		if element.Meta.Parse && (relations[i].Info != nil) && (len(result[i].Tags) > 0) {

			metaInfo.Version = *relations[i].Info.Version
			metaInfo.Timestamp = time.Unix(*relations[i].Info.Timestamp, 0)
			metaInfo.Changeset = *relations[i].Info.Changeset
			metaInfo.Uid = *relations[i].Info.Uid
			metaInfo.User = stringtable[relations[i].GetInfo().GetUserSid()]

			result[i].Tags.AddMetaInfo(metaInfo)
		}
	}
	return result
}

type stringTable []string

func newStringTable(source *osmpbf.StringTable) stringTable {
	result := make(stringTable, len(source.S))
	for i, bytes := range source.S {
		result[i] = string(bytes)
	}
	return result
}
