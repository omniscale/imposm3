package proj

import (
	"goposm/element"
	"math"
)

const pole = 6378137 * math.Pi // 20037508.342789244

func wgsToMerc(long, lat float64) (x, y float64) {
	x = long * pole / 180.0
	y = math.Log(math.Tan((90.0+lat)*math.Pi/360.0)) / math.Pi * pole
	return x, y
}

func mercToWgs(x, y float64) (long, lat float64) {
	long = 180.0 * x / pole
	lat = 180.0 / math.Pi * (2*math.Atan(math.Exp((y/pole)*math.Pi)) - math.Pi/2)
	return long, lat
}

func NodesToMerc(nodes []element.Node) {
	for _, nd := range nodes {
		nd.Long, nd.Lat = wgsToMerc(nd.Long, nd.Lat)
	}
}
