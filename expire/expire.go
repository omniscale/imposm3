package expire

import (
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/proj"
)

type Expireor interface {
	Expire(long, lat float64)
	ExpireNodes(nodes []element.Node, closed bool)
}

func ExpireProjectedNodes(expireor Expireor, nodes []element.Node, srid int, closed bool) {
	if srid == 4326 {
		expireor.ExpireNodes(nodes, closed)
	} else if srid == 3857 {
		nds := make([]element.Node, len(nodes))
		for i, nd := range nodes {
			nds[i].Long, nds[i].Lat = proj.MercToWgs(nd.Long, nd.Lat)
		}
		expireor.ExpireNodes(nds, closed)
	} else {
		panic("unsupported srid")
	}
}

func ExpireProjectedNode(expireor Expireor, node element.Node, srid int) {
	if srid == 4326 {
		expireor.Expire(node.Long, node.Lat)
	} else if srid == 3857 {
		long, lat := proj.MercToWgs(node.Long, node.Lat)
		expireor.Expire(long, lat)
	} else {
		panic("unsupported srid")
	}
}
