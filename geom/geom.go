package geom

import (
	"gogeos"
	"goposm/element"
)

func LineStringWKB(geos *gogeos.GEOS, nodes []element.Node) ([]byte, error) {
	coordSeq, err := geos.CreateCoordSeq(uint32(len(nodes)), 2)
	if err != nil {
		return nil, err
	}
	// coordSeq inherited by LineString
	for i, nd := range nodes {
		coordSeq.SetXY(geos, uint32(i), nd.Long, nd.Lat)
	}
	geom, err := coordSeq.AsLineString(geos)
	if err != nil {
		return nil, err
	}
	defer geos.Destroy(geom)
	return geos.AsWKB(geom)
}

func PolygonWKB(geos *gogeos.GEOS, nodes []element.Node) ([]byte, error) {
	coordSeq, err := geos.CreateCoordSeq(uint32(len(nodes)), 2)
	if err != nil {
		return nil, err
	}
	// coordSeq inherited by LineString, no destroy
	for i, nd := range nodes {
		err := coordSeq.SetXY(geos, uint32(i), nd.Long, nd.Lat)
		if err != nil {
			return nil, err
		}
	}
	geom, err := coordSeq.AsLinearRing(geos)
	if err != nil {
		return nil, err
	}
	// geom inherited by Polygon, no destroy
	geom = geos.CreatePolygon(geom, nil)
	if err != nil {
		return nil, err
	}
	defer geos.Destroy(geom)

	return geos.AsWKB(geom)
}
