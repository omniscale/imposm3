package geom

import (
	"errors"
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/geom/geos"
	"math"
)

type GeomError struct {
	message string
	level   int
}

func (e *GeomError) Error() string {
	return e.message
}

func (e *GeomError) Level() int {
	return e.level
}

func NewGeomError(message string, level int) *GeomError {
	return &GeomError{message, level}
}

var (
	ErrorOneNodeWay = NewGeomError("need at least two separate nodes for way", 0)
	ErrorNoRing     = NewGeomError("linestrings do not form ring", 0)
)

func Point(g *geos.Geos, node element.Node) (*geos.Geom, error) {
	geom := g.Point(node.Long, node.Lat)
	if geom == nil {
		return nil, NewGeomError("couldn't create point", 1)
	}
	g.DestroyLater(geom)
	return geom, nil
}

func nodesEqual(a, b element.Node) bool {
	if d := a.Long - b.Long; math.Abs(d) < 1e-9 {
		if d := a.Lat - b.Lat; math.Abs(d) < 1e-9 {
			return true
		}
	}
	return false
}

func unduplicateNodes(nodes []element.Node) []element.Node {
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

	result := make([]element.Node, 0, len(nodes))
	result = append(result, nodes[0])
	for i := 1; i < len(nodes); i++ {
		if nodesEqual(nodes[i-1], nodes[i]) {
			continue
		}
		result = append(result, nodes[i])
	}
	return result
}

func LineString(g *geos.Geos, nodes []element.Node) (*geos.Geom, error) {
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

func Polygon(g *geos.Geos, nodes []element.Node) (*geos.Geom, error) {
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

func AsGeomElement(g *geos.Geos, geom *geos.Geom) (*element.Geometry, error) {
	wkb := g.AsEwkbHex(geom)
	if wkb == nil {
		return nil, errors.New("could not create wkb")
	}

	return &element.Geometry{
		Wkb:  wkb,
		Geom: geom,
	}, nil
}
