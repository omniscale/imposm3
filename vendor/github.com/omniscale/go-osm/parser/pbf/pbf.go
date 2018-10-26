package pbf

import (
	"time"

	"github.com/omniscale/go-osm"
	"github.com/omniscale/go-osm/parser/pbf/internal/osmpbf"
)

func readDenseNodes(
	dense *osmpbf.DenseNodes,
	block *osmpbf.PrimitiveBlock,
	stringtable stringTable,
	allNodes bool,
	includeMD bool) (coords []osm.Node, nodes []osm.Node) {

	var lastID int64
	var lastLon, lastLat int64

	var lastTimestamp int64
	var lastChangeset int64
	var lastUID int32
	var lastUserSID int32

	coords = make([]osm.Node, len(dense.Id))
	if allNodes {
		nodes = make([]osm.Node, 0, len(dense.Id))
	} else {
		// most nodes have no tags
		nodes = make([]osm.Node, 0, len(dense.Id)/8)
	}
	granularity := int64(block.GetGranularity())
	latOffset := block.GetLatOffset()
	lonOffset := block.GetLonOffset()
	coordScale := 0.000000001
	lastKeyValPos := 0

	var metadata *osm.Metadata

	for i := range coords {
		lastID += dense.Id[i]
		lastLon += dense.Lon[i]
		lastLat += dense.Lat[i]
		coords[i].ID = lastID
		coords[i].Long = (coordScale * float64(lonOffset+(granularity*lastLon)))
		coords[i].Lat = (coordScale * float64(latOffset+(granularity*lastLat)))

		if includeMD {
			lastTimestamp += dense.Denseinfo.Timestamp[i]
			lastChangeset += dense.Denseinfo.Changeset[i]
			lastUID += dense.Denseinfo.Uid[i]
			lastUserSID += dense.Denseinfo.UserSid[i]

			metadata = &osm.Metadata{
				Version:   dense.Denseinfo.Version[i],
				Timestamp: time.Unix(lastTimestamp, 0),
				Changeset: lastChangeset,
				UserID:    lastUID,
				UserName:  stringtable[lastUserSID],
			}
		}
		var tags map[string]string
		addToNodes := allNodes
		if stringtable != nil && len(dense.KeysVals) > 0 {
			if dense.KeysVals[lastKeyValPos] != 0 {
				tags = parseDenseNodeTags(stringtable, &dense.KeysVals, &lastKeyValPos)
				if tags != nil {
					if _, ok := tags["created_by"]; len(tags) > 1 || !ok {
						// don't add nodes with only created_by tag to nodes
						addToNodes = true
					}
				}
			} else {
				lastKeyValPos += 1
			}
		}
		if addToNodes {
			nd := coords[i]
			nd.Tags = tags
			nd.Metadata = metadata
			nodes = append(nodes, nd)
		}
	}

	return coords, nodes
}

func parseDenseNodeTags(stringtable stringTable, keysVals *[]int32, pos *int) map[string]string {
	// make map later if needed
	var result map[string]string
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
			result = make(map[string]string)
		}
		result[stringtable[key]] = stringtable[val]
	}
}

func parseTags(stringtable stringTable, keys []uint32, vals []uint32) map[string]string {
	if len(keys) == 0 {
		return nil
	}
	tags := make(map[string]string, len(keys))
	for i := 0; i < len(keys); i++ {
		key := stringtable[keys[i]]
		val := stringtable[vals[i]]
		tags[key] = val
	}
	return tags
}

func readNodes(
	nodes []osmpbf.Node,
	block *osmpbf.PrimitiveBlock,
	stringtable stringTable,
	allNodes bool,
	includeMD bool,
) ([]osm.Node, []osm.Node) {

	coords := make([]osm.Node, len(nodes))
	nds := make([]osm.Node, 0, len(nodes)/8)
	granularity := int64(block.GetGranularity())
	latOffset := block.GetLatOffset()
	lonOffset := block.GetLonOffset()
	coordScale := 0.000000001

	var metadata *osm.Metadata

	for i := range nodes {
		id := nodes[i].Id
		lon := nodes[i].Lon
		lat := nodes[i].Lat
		coords[i].ID = id
		coords[i].Long = (coordScale * float64(lonOffset+(granularity*lon)))
		coords[i].Lat = (coordScale * float64(latOffset+(granularity*lat)))
		var tags map[string]string
		addToNodes := allNodes
		if includeMD {
			version := int32(0)
			if nodes[i].Info.Version != nil {
				version = *nodes[i].Info.Version
			}
			metadata = &osm.Metadata{
				Version:   version,
				Timestamp: time.Unix(nodes[i].Info.Timestamp, 0),
				Changeset: nodes[i].Info.Changeset,
				UserID:    nodes[i].Info.Uid,
				UserName:  stringtable[nodes[i].Info.UserSid],
			}
		}
		if stringtable != nil {
			tags = parseTags(stringtable, nodes[i].Keys, nodes[i].Vals)
			if tags != nil {
				if _, ok := tags["created_by"]; len(tags) > 1 || !ok {
					// don't add nodes with only created_by tag to nodes
					addToNodes = true
				}
			}
		}
		if addToNodes {
			nd := coords[i]
			nd.Tags = tags
			nd.Metadata = metadata
			nds = append(nds, nd)
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
	ways []osmpbf.Way,
	block *osmpbf.PrimitiveBlock,
	stringtable stringTable,
	includeMD bool,
) []osm.Way {

	result := make([]osm.Way, len(ways))

	for i := range ways {
		id := ways[i].Id
		result[i].ID = id
		result[i].Tags = parseTags(stringtable, ways[i].Keys, ways[i].Vals)
		result[i].Refs = parseDeltaRefs(ways[i].Refs)
		if includeMD {
			version := int32(0)
			if ways[i].Info.Version != nil {
				version = *ways[i].Info.Version
			}
			metadata := &osm.Metadata{
				Version:   version,
				Timestamp: time.Unix(ways[i].Info.Timestamp, 0),
				Changeset: ways[i].Info.Changeset,
				UserID:    ways[i].Info.Uid,
				UserName:  stringtable[ways[i].Info.UserSid],
			}
			result[i].Metadata = metadata
		}
	}
	return result
}

func parseRelationMembers(rel osmpbf.Relation, stringtable stringTable) []osm.Member {
	result := make([]osm.Member, len(rel.Memids))

	var lastID int64
	for i := range rel.Memids {
		lastID += rel.Memids[i]
		result[i].ID = lastID
		result[i].Role = stringtable[rel.RolesSid[i]]
		result[i].Type = osm.MemberType(rel.Types[i])
	}
	return result
}

func readRelations(
	relations []osmpbf.Relation,
	block *osmpbf.PrimitiveBlock,
	stringtable stringTable,
	includeMD bool,
) []osm.Relation {

	result := make([]osm.Relation, len(relations))

	for i := range relations {
		id := relations[i].Id
		result[i].ID = id
		result[i].Tags = parseTags(stringtable, relations[i].Keys, relations[i].Vals)
		result[i].Members = parseRelationMembers(relations[i], stringtable)
		if includeMD {
			version := int32(0)
			if relations[i].Info.Version != nil {
				version = *relations[i].Info.Version
			}
			metadata := &osm.Metadata{
				Version:   version,
				Timestamp: time.Unix(relations[i].Info.Timestamp, 0),
				Changeset: relations[i].Info.Changeset,
				UserID:    relations[i].Info.Uid,
				UserName:  stringtable[relations[i].Info.UserSid],
			}
			result[i].Metadata = metadata
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
