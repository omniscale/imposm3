package binary

import (
	"code.google.com/p/goprotobuf/proto"

	"goposm/element"
	"goposm/model"
)

// struct MarshalError {
//     msg string
// }

func Marshal(elem interface{}) ([]byte, error) {
	switch typedElem := elem.(type) {
	case element.Node:
		return MarshalNode(typedElem)
	default:
		panic("invalid elem to marshal")
	}

	return []byte{}, nil
}

func MarshalNode(node element.Node) ([]byte, error) {
	pbfNode := &model.Node{}
	foo := int64(node.Id)
	pbfNode.Id = &foo
	pbfNode.FromWgsCoord(node.Long, node.Lat)
	return proto.Marshal(pbfNode)
}
