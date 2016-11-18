package expire

import "github.com/omniscale/imposm3/element"

type Expireor interface {
	Expire(long, lat float64)
}

func ExpireNodes(expireor Expireor, nodes []element.Node) {
	switch expireor := expireor.(type) {
	default:
		for _, nd := range nodes {
			expireor.Expire(nd.Long, nd.Lat)
		}
	case *TileExpireor:
		expireor.ExpireLinestring(nodes)
	}
}

type NoExpireor struct{}

func (_ NoExpireor) Expire(long, lat float64) {}
