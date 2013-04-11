package binary

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	bin "encoding/binary"
	"goposm/element"
	"goposm/model"
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

	return []byte{}, nil
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
	pbfNode := &model.Node{}
	nodeId := node.Id
	pbfNode.Id = &nodeId
	pbfNode.FromWgsCoord(node.Long, node.Lat)
	pbfNode.Tags = node.TagsAsArray()
	return proto.Marshal(pbfNode)
}

func UnmarshalNode(data []byte) (node *element.Node, err error) {
	pbfNode := &model.Node{}
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

func MarshalWay(way *element.Way) ([]byte, error) {
	pbfWay := &model.Way{}
	pbfWay.Id = &way.Id
	pbfWay.Nodes = way.Nodes
	pbfWay.Tags = way.TagsAsArray()
	return proto.Marshal(pbfWay)
}

func UnmarshalWay(data []byte) (way *element.Way, err error) {
	pbfWay := &model.Way{}
	err = proto.Unmarshal(data, pbfWay)
	if err != nil {
		return nil, err
	}

	way = &element.Way{}
	way.Id = *pbfWay.Id
	way.Nodes = pbfWay.Nodes
	way.TagsFromArray(pbfWay.Tags)
	return way, nil
}

func MarshalRelation(relation *element.Relation) ([]byte, error) {
	pbfRelation := &model.Relation{}
	pbfRelation.Id = &relation.Id
	//pbfRelation.Members = relation.Members
	pbfRelation.Tags = relation.TagsAsArray()
	return proto.Marshal(pbfRelation)
}

func UnmarshalRelation(data []byte) (relation *element.Relation, err error) {
	pbfRelation := &model.Relation{}
	err = proto.Unmarshal(data, pbfRelation)
	if err != nil {
		return nil, err
	}

	relation = &element.Relation{}
	relation.Id = *pbfRelation.Id
	//relation.Nodes = pbfRelation.Node
	relation.TagsFromArray(pbfRelation.Tags)
	return relation, nil
}
