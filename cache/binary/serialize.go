package binary

import (
	"github.com/gregtzar/imposm3/element"
)

const COORD_FACTOR float64 = 11930464.7083 // ((2<<31)-1)/360.0

func CoordToInt(coord float64) uint32 {
	return uint32((coord + 180.0) * COORD_FACTOR)
}

func IntToCoord(coord uint32) float64 {
	return float64((float64(coord) / COORD_FACTOR) - 180.0)
}

func MarshalNode(node *element.Node) ([]byte, error) {
	pbfNode := &Node{}
	pbfNode.fromWgsCoord(node.Long, node.Lat)
	pbfNode.Tags = tagsAsArray(node.Tags)
	return pbfNode.Marshal()
}

func UnmarshalNode(data []byte) (node *element.Node, err error) {
	pbfNode := &Node{}
	err = pbfNode.Unmarshal(data)
	if err != nil {
		return nil, err
	}

	node = &element.Node{}
	node.Long, node.Lat = pbfNode.wgsCoord()
	node.Tags = tagsFromArray(pbfNode.Tags)
	return node, nil
}

func deltaPack(data []int64) {
	if len(data) < 2 {
		return
	}
	lastVal := data[0]
	for i := 1; i < len(data); i++ {
		data[i], lastVal = data[i]-lastVal, data[i]
	}
}

func deltaUnpack(data []int64) {
	if len(data) < 2 {
		return
	}
	for i := 1; i < len(data); i++ {
		data[i] = data[i] + data[i-1]
	}
}

func MarshalWay(way *element.Way) ([]byte, error) {
	// TODO reuse Way to avoid make(Tags) for each way in tagsAsArray
	pbfWay := &Way{}
	deltaPack(way.Refs)
	pbfWay.Refs = way.Refs
	pbfWay.Tags = tagsAsArray(way.Tags)
	return pbfWay.Marshal()
}

func UnmarshalWay(data []byte) (way *element.Way, err error) {
	pbfWay := &Way{}
	err = pbfWay.Unmarshal(data)
	if err != nil {
		return nil, err
	}

	way = &element.Way{}
	deltaUnpack(pbfWay.Refs)
	way.Refs = pbfWay.Refs
	way.Tags = tagsFromArray(pbfWay.Tags)
	return way, nil
}

func MarshalRelation(relation *element.Relation) ([]byte, error) {
	pbfRelation := &Relation{}
	pbfRelation.MemberIds = make([]int64, len(relation.Members))
	pbfRelation.MemberTypes = make([]Relation_MemberType, len(relation.Members))
	pbfRelation.MemberRoles = make([]string, len(relation.Members))
	for i, m := range relation.Members {
		pbfRelation.MemberIds[i] = m.Id
		pbfRelation.MemberTypes[i] = Relation_MemberType(m.Type)
		pbfRelation.MemberRoles[i] = m.Role
	}
	pbfRelation.Tags = tagsAsArray(relation.Tags)
	return pbfRelation.Marshal()
}

func UnmarshalRelation(data []byte) (relation *element.Relation, err error) {
	pbfRelation := &Relation{}
	err = pbfRelation.Unmarshal(data)
	if err != nil {
		return nil, err
	}

	relation = &element.Relation{}
	relation.Members = make([]element.Member, len(pbfRelation.MemberIds))
	for i, _ := range pbfRelation.MemberIds {
		relation.Members[i].Id = pbfRelation.MemberIds[i]
		relation.Members[i].Type = element.MemberType(pbfRelation.MemberTypes[i])
		relation.Members[i].Role = pbfRelation.MemberRoles[i]
	}
	//relation.Nodes = pbfRelation.Node
	relation.Tags = tagsFromArray(pbfRelation.Tags)
	return relation, nil
}
