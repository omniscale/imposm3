package geom

import (
	"gogeos"
	"goposm/element"
)

func LineString(nodes []element.Node) []byte {
	geos := gogeos.NewGEOS()
	defer geos.Finish()

	coordSeq := geos.CreateCoordSeq(uint32(len(nodes)), 2)
	for i, nd := range nodes {
		coordSeq.SetXY(geos, uint32(i), nd.Long, nd.Lat)
	}
	geom := coordSeq.AsLineString(geos)
	defer geos.Destroy(geom)
	return geos.AsWKB(geom)
}
