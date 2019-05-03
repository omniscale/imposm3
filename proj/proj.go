package proj

import (
	"math"

	osm "github.com/omniscale/go-osm"
)

const pole = 6378137 * math.Pi // 20037508.342789244

func WgsToMerc(long, lat float64) (x, y float64) {
	x = long * pole / 180.0
	y = math.Log(math.Tan((90.0+lat)*math.Pi/360.0)) / math.Pi * pole
	return x, y
}

func MercToWgs(x, y float64) (long, lat float64) {
	long = 180.0 * x / pole
	lat = 180.0 / math.Pi * (2*math.Atan(math.Exp((y/pole)*math.Pi)) - math.Pi/2)
	return long, lat
}

func NodesToMerc(nodes []osm.Node) {
	for i, nd := range nodes {
		nodes[i].Long, nodes[i].Lat = WgsToMerc(nd.Long, nd.Lat)
	}
}

func NodeToMerc(node *osm.Node) {
	node.Long, node.Lat = WgsToMerc(node.Long, node.Lat)
}
