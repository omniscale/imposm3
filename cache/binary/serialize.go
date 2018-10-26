package binary

import osm "github.com/omniscale/go-osm"

const COORD_FACTOR float64 = 11930464.7083 // ((2<<31)-1)/360.0

func CoordToInt(coord float64) uint32 {
	return uint32((coord + 180.0) * COORD_FACTOR)
}

func IntToCoord(coord uint32) float64 {
	return float64((float64(coord) / COORD_FACTOR) - 180.0)
}

func MarshalNode(node *osm.Node) ([]byte, error) {
	pbfNode := &Node{}
	pbfNode.fromWgsCoord(node.Long, node.Lat)
	pbfNode.Tags = tagsAsArray(node.Tags)
	return pbfNode.Marshal()
}

func UnmarshalNode(data []byte) (node *osm.Node, err error) {
	pbfNode := &Node{}
	err = pbfNode.Unmarshal(data)
	if err != nil {
		return nil, err
	}

	node = &osm.Node{}
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

func MarshalWay(way *osm.Way) ([]byte, error) {
	// TODO reuse Way to avoid make(Tags) for each way in tagsAsArray
	pbfWay := &Way{}
	deltaPack(way.Refs)
	pbfWay.Refs = way.Refs
	pbfWay.Tags = tagsAsArray(way.Tags)
	return pbfWay.Marshal()
}

func UnmarshalWay(data []byte) (way *osm.Way, err error) {
	pbfWay := &Way{}
	err = pbfWay.Unmarshal(data)
	if err != nil {
		return nil, err
	}

	way = &osm.Way{}
	deltaUnpack(pbfWay.Refs)
	way.Refs = pbfWay.Refs
	way.Tags = tagsFromArray(pbfWay.Tags)
	return way, nil
}

func MarshalRelation(relation *osm.Relation) ([]byte, error) {
	pbfRelation := &Relation{}
	pbfRelation.MemberIds = make([]int64, len(relation.Members))
	pbfRelation.MemberTypes = make([]Relation_MemberType, len(relation.Members))
	pbfRelation.MemberRoles = make([]string, len(relation.Members))
	for i, m := range relation.Members {
		pbfRelation.MemberIds[i] = m.ID
		pbfRelation.MemberTypes[i] = Relation_MemberType(m.Type)
		pbfRelation.MemberRoles[i] = m.Role
	}
	pbfRelation.Tags = tagsAsArray(relation.Tags)
	return pbfRelation.Marshal()
}

func UnmarshalRelation(data []byte) (relation *osm.Relation, err error) {
	pbfRelation := &Relation{}
	err = pbfRelation.Unmarshal(data)
	if err != nil {
		return nil, err
	}

	relation = &osm.Relation{}
	relation.Members = make([]osm.Member, len(pbfRelation.MemberIds))
	for i, _ := range pbfRelation.MemberIds {
		relation.Members[i].ID = pbfRelation.MemberIds[i]
		relation.Members[i].Type = osm.MemberType(pbfRelation.MemberTypes[i])
		relation.Members[i].Role = pbfRelation.MemberRoles[i]
	}
	//relation.Nodes = pbfRelation.Node
	relation.Tags = tagsFromArray(pbfRelation.Tags)
	return relation, nil
}
