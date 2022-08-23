package geom

import (
	"errors"
	"math"
	"runtime"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/geom/geos"
)

type GeometryError struct {
	message string
	level   int
}

type Geometry struct {
	Geom *geos.Geom
	Wkb  []byte
}

func (e *GeometryError) Error() string {
	return e.message
}

func (e *GeometryError) Level() int {
	return e.level
}

func newGeometryError(message string, level int) *GeometryError {
	return &GeometryError{message, level}
}

var (
	ErrorOneNodeWay = newGeometryError("need at least two separate nodes for way", 0)
	ErrorNoRing     = newGeometryError("linestrings do not form ring", 0)
)

func Point(g *geos.Geos, node osm.Node) (*geos.Geom, error) {
	geom := g.Point(node.Long, node.Lat)
	if geom == nil {
		return nil, newGeometryError("couldn't create point", 1)
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

func MultiLinestring(rel *osm.Relation, srid int) (*geos.Geom, error) {
	g := geos.NewGeos()
	g.SetHandleSrid(srid)
	defer g.Finish()

	var lines []*geos.Geom

	for _, member := range rel.Members {
		if member.Way == nil {
			continue
		}

		line, err := LineString(g, member.Way.Nodes)

		if err != nil {
			return nil, err
		}

		if line != nil {
			// Clear the finalizer created in LineString()
			// as we want to make the object a part of MultiLineString.
			runtime.SetFinalizer(line, nil)
			lines = append(lines, line)
		}
	}

	result := g.MultiLineString(lines)
	if result == nil {
		return nil, errors.New("Error while building multi-linestring.")
	}

	g.DestroyLater(result)

	return result, nil
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
