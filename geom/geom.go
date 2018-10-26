package geom

import (
	"errors"
	"math"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/geom/geos"
)

type GeomError struct {
	message string
	level   int
}

type Geometry struct {
	Geom *geos.Geom
	Wkb  []byte
}

func (e *GeomError) Error() string {
	return e.message
}

func (e *GeomError) Level() int {
	return e.level
}

func newGeomError(message string, level int) *GeomError {
	return &GeomError{message, level}
}

var (
	ErrorOneNodeWay = newGeomError("need at least two separate nodes for way", 0)
	ErrorNoRing     = newGeomError("linestrings do not form ring", 0)
)

func Point(g *geos.Geos, node osm.Node) (*geos.Geom, error) {
	geom := g.Point(node.Long, node.Lat)
	if geom == nil {
		return nil, newGeomError("couldn't create point", 1)
	}
	g.DestroyLater(geom)
	return geom, nil
}

func nodesEqual(a, b osm.Node) bool {
	if d := a.Long - b.Long; math.Abs(d) < 1e-9 {
		if d := a.Lat - b.Lat; math.Abs(d) < 1e-9 {
			return true
		}
	}
	return false
}

func unduplicateNodes(nodes []osm.Node) []osm.Node {
	if len(nodes) < 2 {
		return nodes
	}
	foundDup := false
	for i := 1; i < len(nodes); i++ {
		if nodesEqual(nodes[i-1], nodes[i]) {
			foundDup = true
			break
		}
	}
	if !foundDup {
		return nodes
	}

	result := make([]osm.Node, 0, len(nodes))
	result = append(result, nodes[0])
	for i := 1; i < len(nodes); i++ {
		if nodesEqual(nodes[i-1], nodes[i]) {
			continue
		}
		result = append(result, nodes[i])
	}
	return result
}

func LineString(g *geos.Geos, nodes []osm.Node) (*geos.Geom, error) {
	nodes = unduplicateNodes(nodes)
	if len(nodes) < 2 {
		return nil, ErrorOneNodeWay
	}

	coordSeq, err := g.CreateCoordSeq(uint32(len(nodes)), 2)
	if err != nil {
		return nil, err
	}
	// coordSeq inherited by LineString
	for i, nd := range nodes {
		coordSeq.SetXY(g, uint32(i), nd.Long, nd.Lat)
	}
	geom, err := coordSeq.AsLineString(g)
	if err != nil {
		// coordSeq gets Destroy by GEOS
		return nil, err
	}
	g.DestroyLater(geom)
	return geom, nil
}

func Polygon(g *geos.Geos, nodes []osm.Node) (*geos.Geom, error) {
	nodes = unduplicateNodes(nodes)
	if len(nodes) < 4 {
		return nil, ErrorNoRing
	}

	coordSeq, err := g.CreateCoordSeq(uint32(len(nodes)), 2)
	if err != nil {
		return nil, err
	}

	// coordSeq inherited by LinearRing, no destroy
	for i, nd := range nodes {
		err := coordSeq.SetXY(g, uint32(i), nd.Long, nd.Lat)
		if err != nil {
			return nil, err
		}
	}
	ring, err := coordSeq.AsLinearRing(g)
	if err != nil {
		// coordSeq gets Destroy by GEOS
		return nil, err
	}
	// ring inherited by Polygon, no destroy

	geom := g.Polygon(ring, nil)
	if geom == nil {
		g.Destroy(ring)
		return nil, errors.New("unable to create polygon")
	}
	g.DestroyLater(geom)
	return geom, nil
}

func AsGeomElement(g *geos.Geos, geom *geos.Geom) (Geometry, error) {
	wkb := g.AsEwkbHex(geom)
	if wkb == nil {
		return Geometry{}, errors.New("could not create wkb")
	}

	return Geometry{
		Wkb:  wkb,
		Geom: geom,
	}, nil
}
