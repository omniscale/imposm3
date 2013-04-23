package parser

import (
	"fmt"
	"goposm/element"
	"log"
	"os"
	"osmpbf"
)

type PBF struct {
	file     *os.File
	filename string
	offset   int64
}

type BlockPosition struct {
	filename string
	offset   int64
	size     int32
}

func Open(filename string) (f *PBF, err error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	f = &PBF{filename: filename, file: file}
	return f, nil
}

func (pbf *PBF) Close() error {
	return pbf.file.Close()
}

func (pbf *PBF) NextDataPosition() (offset int64, size int32) {
	header := pbf.nextBlobHeader()
	size = header.GetDatasize()
	offset = pbf.offset

	pbf.offset += int64(size)
	pbf.file.Seek(pbf.offset, 0)

	if header.GetType() == "OSMHeader" {
		return pbf.NextDataPosition()
	}
	return
}

const COORD_FACTOR float64 = 11930464.7083 // ((2<<31)-1)/360.0

func coordToInt(coord float64) uint32 {
	return uint32((coord + 180.0) * COORD_FACTOR)
}

func intToCoord(coord uint32) float64 {
	return float64((float64(coord) / COORD_FACTOR) - 180.0)
}

func ReadDenseNodes(
	dense *osmpbf.DenseNodes,
	block *osmpbf.PrimitiveBlock,
	stringtable StringTable) (nodes []element.Node) {

	var lastId int64
	var lastLon, lastLat int64
	nodes = make([]element.Node, len(dense.Id))
	granularity := int64(block.GetGranularity())
	latOffset := block.GetLatOffset()
	lonOffset := block.GetLonOffset()
	coordScale := 0.000000001
	lastKeyValPos := 0

	for i := range nodes {
		lastId += dense.Id[i]
		lastLon += dense.Lon[i]
		lastLat += dense.Lat[i]
		nodes[i].Id = lastId
		nodes[i].Long = (coordScale * float64(lonOffset+(granularity*lastLon)))
		nodes[i].Lat = (coordScale * float64(latOffset+(granularity*lastLat)))
		if dense.KeysVals[lastKeyValPos] != 0 {
			nodes[i].Tags = ParseDenseNodeTags(stringtable, &dense.KeysVals, &lastKeyValPos)
		} else {
			lastKeyValPos += 1
		}
	}
	return nodes
}

func ParseDenseNodeTags(stringtable StringTable, keysVals *[]int32, pos *int) map[string]string {
	result := make(map[string]string)
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
		result[stringtable[key]] = stringtable[val]
	}
	return result
}
func ParseTags(stringtable StringTable, keys []uint32, vals []uint32) map[string]string {
	tags := make(map[string]string)
	for i := 0; i < len(keys); i++ {
		key := stringtable[keys[i]]
		val := stringtable[vals[i]]
		tags[key] = val
	}
	return tags
}

func ReadNodes(
	nodes []*osmpbf.Node,
	block *osmpbf.PrimitiveBlock,
	stringtable StringTable) []element.Node {

	result := make([]element.Node, len(nodes))
	granularity := int64(block.GetGranularity())
	latOffset := block.GetLatOffset()
	lonOffset := block.GetLonOffset()
	coordScale := 0.000000001

	for i := range nodes {
		id := *nodes[i].Id
		lon := *nodes[i].Lon
		lat := *nodes[i].Lat
		result[i].Id = id
		result[i].Long = (coordScale * float64(lonOffset+(granularity*lon)))
		result[i].Lat = (coordScale * float64(latOffset+(granularity*lat)))
		result[i].Tags = ParseTags(stringtable, nodes[i].Keys, nodes[i].Vals)
	}
	return result
}

func ParseDeltaRefs(refs []int64) []int64 {
	result := make([]int64, len(refs))
	var lastRef int64

	for i, refDelta := range refs {
		lastRef += refDelta
		result[i] = lastRef
	}
	return result
}

func ReadWays(
	ways []*osmpbf.Way,
	block *osmpbf.PrimitiveBlock,
	stringtable StringTable) []element.Way {

	result := make([]element.Way, len(ways))

	for i := range ways {
		id := *ways[i].Id
		result[i].Id = id
		result[i].Tags = ParseTags(stringtable, ways[i].Keys, ways[i].Vals)
		result[i].Refs = ParseDeltaRefs(ways[i].Refs)
	}
	return result
}

func ParseRelationMembers(rel *osmpbf.Relation, stringtable StringTable) []element.Member {
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

func ReadRelations(
	relations []*osmpbf.Relation,
	block *osmpbf.PrimitiveBlock,
	stringtable StringTable) []element.Relation {

	result := make([]element.Relation, len(relations))

	for i := range relations {
		id := *relations[i].Id
		result[i].Id = id
		result[i].Tags = ParseTags(stringtable, relations[i].Keys, relations[i].Vals)
		result[i].Members = ParseRelationMembers(relations[i], stringtable)
	}
	return result
}

type StringTable []string

func NewStringTable(source *osmpbf.StringTable) StringTable {
	result := make(StringTable, len(source.S))
	for i, bytes := range source.S {
		result[i] = string(bytes)
	}
	return result
}

func PBFBlockPositions(filename string) chan BlockPosition {
	pbf, err := Open(filename)
	if err != nil {
		log.Fatal(err)
	}

	return pbf.BlockPositions()
}

func ParseBlock(pos BlockPosition, nodes chan []element.Node, ways chan []element.Way, relations chan []element.Relation) {
	block := ReadPrimitiveBlock(pos)
	stringtable := NewStringTable(block.GetStringtable())

	for _, group := range block.Primitivegroup {
		dense := group.GetDense()
		if dense != nil {
			parsedNodes := ReadDenseNodes(dense, block, stringtable)
			if len(parsedNodes) > 0 {
				nodes <- parsedNodes
			}
		}
		parsedNodes := ReadNodes(group.Nodes, block, stringtable)
		if len(parsedNodes) > 0 {
			nodes <- parsedNodes
		}
		parsedWays := ReadWays(group.Ways, block, stringtable)
		if len(parsedWays) > 0 {
			ways <- parsedWays
		}
		parsedRelations := ReadRelations(group.Relations, block, stringtable)
		if len(parsedRelations) > 0 {
			relations <- parsedRelations
		}
	}

}

func PBFStats(filename string) {
	pbf, err := Open(filename)
	if err != nil {
		log.Fatal(err)
	}

	var nodesCounter, relationsCounter, waysCounter int

	for pos := range pbf.BlockPositions() {
		block := ReadPrimitiveBlock(pos)
		stringtable := NewStringTable(block.GetStringtable())

		for _, group := range block.Primitivegroup {
			dense := group.GetDense()
			if dense != nil {
				_ = ReadDenseNodes(dense, block, stringtable)
				nodesCounter += len(dense.Id)
			}
			_ = ReadNodes(group.Nodes, block, stringtable)
			nodesCounter += len(group.Nodes)
			waysCounter += len(group.Ways)
			_ = ReadWays(group.Ways, block, stringtable)
			relationsCounter += len(group.Relations)
			_ = ReadRelations(group.Relations, block, stringtable)

		}
	}
	fmt.Printf("nodes: %v\tways: %v\trelations:%v\n", nodesCounter, waysCounter, relationsCounter)
}
