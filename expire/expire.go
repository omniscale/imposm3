package expire

import (
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/proj"
)

type Expireor interface {
	Expire(long, lat float64)
}

func ExpireNodes(expireor Expireor, nodes []element.Node, srid int) {
	if srid == 4326 {
		for _, nd := range nodes {
			expireor.Expire(nd.Long, nd.Lat)
		}
	} else if srid == 4326 {
		for _, nd := range nodes {
			expireor.Expire(proj.MercToWgs(nd.Long, nd.Lat))
		}
	} else {
		panic("unsupported srid")
	}
}
