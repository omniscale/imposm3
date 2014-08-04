package expire

import (
	"github.com/omniscale/imposm3/element"
)

type Expireor interface {
	Expire(long, lat float64)
}

func ExpireNodes(expireor Expireor, nodes []element.Node) {
	for _, nd := range nodes {
		expireor.Expire(nd.Long, nd.Lat)
	}
}
