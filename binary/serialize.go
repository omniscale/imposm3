package binary

import (
	"code.google.com/p/goprotobuf/proto"
	"goposm/element"
	"goposm/model"
)

// struct MarshalError {
//     msg string
// }

func tagsFromArray(arr []string) *element.Tags {
	result := make(element.Tags)
	for i := 0; i < len(arr); i += 2 {
		result[arr[i]] = arr[i+1]
	}
	return &result
}

func tagsAsArray(tags *element.Tags) []string {
	result := make([]string, 0, 2*len(*tags))
	for key, val := range *tags {
		result = append(result, key, val)
	}
	return result
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

func MarshalNode(node *element.Node) ([]byte, error) {
	pbfNode := &model.Node{}
	nodeId := node.Id
	pbfNode.Id = &nodeId
	pbfNode.FromWgsCoord(node.Long, node.Lat)
	pbfNode.Tags = tagsAsArray(&node.Tags)
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
	node.Tags = *tagsFromArray(pbfNode.Tags)
	return node, nil
}

func MarshalWay(way *element.Way) ([]byte, error) {
	pbfWay := &model.Way{}
	pbfWay.Id = &way.Id
	pbfWay.Nodes = way.Nodes
	pbfWay.Tags = tagsAsArray(&way.Tags)
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
	way.Tags = *tagsFromArray(pbfWay.Tags)
	return way, nil
}
