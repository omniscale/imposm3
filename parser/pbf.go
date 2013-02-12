package parser

import (
	"fmt"
	"goposm/binary"
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

func DenseNodeTags(stringtable []string, keyvals []int32) (tags map[string]string, nextPos int) {
	tags = make(map[string]string)
	nextPos = 0
	for {
		keyId := keyvals[nextPos]
		nextPos += 1
		if keyId == 0 {
			return
		}
		key := stringtable[keyId]
		valId := keyvals[nextPos]
		nextPos += 1
		val := stringtable[valId]

		tags[key] = val
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
	stringtable *StringTable) (nodes []element.Node) {

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
		nodes[i].Id = element.OSMID(lastId)
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

func ParseDenseNodeTags(stringtable *StringTable, keysVals *[]int32, pos *int) map[string]string {
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
		result[(*stringtable)[key]] = (*stringtable)[val]
	}
	return result
}

type StringTable []string

func NewStringTable(source *osmpbf.StringTable) *StringTable {
	result := make(StringTable, len(source.S))
	for i, bytes := range source.S {
		result[i] = string(bytes)
	}
	return &result
}

func BlockPositions(filename string) {
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
				nodes := ReadDenseNodes(dense, block, stringtable)
				lon, lat := nodes[0].Long, nodes[0].Lat
				data, _ := binary.Marshal(nodes[0])
				fmt.Printf("len: %d", len(data))
				fmt.Printf("%v", data)
				fmt.Printf("%12d %10.8f %10.8f\n", nodes[0].Id, lon, lat)
				nodesCounter += len(dense.Id)
			}
			nodesCounter += len(group.Nodes)
			waysCounter += len(group.Ways)
			relationsCounter += len(group.Relations)
		}
	}
	fmt.Printf("nodes: %v\tways: %v\trelations:%v\n", nodesCounter, waysCounter, relationsCounter)
}
