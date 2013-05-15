package binary

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	bin "encoding/binary"
	"goposm/element"
)

const COORD_FACTOR float64 = 11930464.7083 // ((2<<31)-1)/360.0

func CoordToInt(coord float64) uint32 {
	return uint32((coord + 180.0) * COORD_FACTOR)
}

func IntToCoord(coord uint32) float64 {
	return float64((float64(coord) / COORD_FACTOR) - 180.0)
}

func Marshal(elem interface{}) ([]byte, error) {
	switch typedElem := elem.(type) {
	case element.Node:
		return MarshalNode(&typedElem)
	default:
		panic("invalid elem to marshal")
	}
}

func MarshalCoord(node *element.Node) ([]byte, error) {
	data := make([]byte, 8)

	buf := bytes.NewBuffer(data)
	err := bin.Write(buf, bin.LittleEndian, CoordToInt(node.Long))
	if err != nil {
		return nil, err
	}
	err = bin.Write(buf, bin.LittleEndian, CoordToInt(node.Lat))
	if err != nil {
		return nil, err
	}
	return data, nil
}

func UnmarshalCoord(id int64, data []byte) (node *element.Node, err error) {
	var long, lat uint32
	buf := bytes.NewBuffer(data)
	err = bin.Read(buf, bin.LittleEndian, &long)
	if err != nil {
		return nil, err
	}
	err = bin.Read(buf, bin.LittleEndian, &lat)
	if err != nil {
		return nil, err
	}

	node = &element.Node{}
	node.Id = id
	node.Long = IntToCoord(long)
	node.Lat = IntToCoord(lat)
	return node, nil
}

func MarshalNode(node *element.Node) ([]byte, error) {
	pbfNode := &Node{}
	nodeId := node.Id
	pbfNode.Id = &nodeId
	pbfNode.FromWgsCoord(node.Long, node.Lat)
	pbfNode.Tags = node.TagsAsArray()
	return proto.Marshal(pbfNode)
}

func UnmarshalNode(data []byte) (node *element.Node, err error) {
	pbfNode := &Node{}
	err = proto.Unmarshal(data, pbfNode)
	if err != nil {
		return nil, err
	}

	node = &element.Node{}
	node.Id = *pbfNode.Id
	node.Long, node.Lat = pbfNode.WgsCoord()
	node.TagsFromArray(pbfNode.Tags)
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
	// TODO reuse Way to avoid make(Tags) for each way in TagsAsArray
	pbfWay := &Way{}
	pbfWay.Id = &way.Id
	deltaPack(way.Refs)
	pbfWay.Refs = way.Refs
	pbfWay.Tags = way.TagsAsArray()
	return proto.Marshal(pbfWay)
}

func UnmarshalWay(data []byte) (way *element.Way, err error) {
	pbfWay := &Way{}
	err = proto.Unmarshal(data, pbfWay)
	if err != nil {
		return nil, err
	}

	way = &element.Way{}
	way.Id = *pbfWay.Id
	deltaUnpack(pbfWay.Refs)
	way.Refs = pbfWay.Refs
	way.TagsFromArray(pbfWay.Tags)
	return way, nil
}

func MarshalRelation(relation *element.Relation) ([]byte, error) {
	pbfRelation := &Relation{}
	pbfRelation.Id = &relation.Id
	// TODO store members
	pbfRelation.MemberIds = make([]int64, len(relation.Members))
	pbfRelation.MemberTypes = make([]Relation_MemberType, len(relation.Members))
	pbfRelation.MemberRoles = make([]string, len(relation.Members))
	for i, m := range relation.Members {
		pbfRelation.MemberIds[i] = m.Id
		pbfRelation.MemberTypes[i] = Relation_MemberType(m.Type)
		pbfRelation.MemberRoles[i] = m.Role
	}
	pbfRelation.Tags = relation.TagsAsArray()
	return proto.Marshal(pbfRelation)
}

func UnmarshalRelation(data []byte) (relation *element.Relation, err error) {
	pbfRelation := &Relation{}
	err = proto.Unmarshal(data, pbfRelation)
	if err != nil {
		return nil, err
	}

	relation = &element.Relation{}
	relation.Id = *pbfRelation.Id
	relation.Members = make([]element.Member, len(pbfRelation.MemberIds))
	for i, _ := range pbfRelation.MemberIds {
		relation.Members[i].Id = pbfRelation.MemberIds[i]
		relation.Members[i].Type = element.MemberType(pbfRelation.MemberTypes[i])
		relation.Members[i].Role = pbfRelation.MemberRoles[i]
	}
	//relation.Nodes = pbfRelation.Node
	relation.TagsFromArray(pbfRelation.Tags)
	return relation, nil
}
