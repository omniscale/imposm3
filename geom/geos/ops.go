package geos

/*
#cgo LDFLAGS: -lgeos_c
#include "geos_c.h"
#include <stdlib.h>
*/
import "C"

func (g *Geos) Contains(a, b *Geom) bool {
	result := C.GEOSContains_r(g.v, a.v, b.v)
	if result == 1 {
		return true
	}
	// result == 2 -> exception (already logged to console)
	return false
}

func (g *Geos) Intersects(a, b *Geom) bool {
	result := C.GEOSIntersects_r(g.v, a.v, b.v)
	if result == 1 {
		return true
	}
	// result == 2 -> exception (already logged to console)
	return false
}

func (g *Geos) Intersection(a, b *Geom) *Geom {
	result := C.GEOSIntersection_r(g.v, a.v, b.v)
	if result == nil {
		return nil
	}
	geom := &Geom{result}
	return geom
}

func (g *Geos) Buffer(geom *Geom, size float64) *Geom {
	buffered := C.GEOSBuffer_r(g.v, geom.v, C.double(size), 50)
	if buffered == nil {
		return nil
	}
	return &Geom{buffered}
}

func (g *Geos) SimplifyPreserveTopology(geom *Geom, tolerance float64) *Geom {
	simplified := C.GEOSTopologyPreserveSimplify_r(g.v, geom.v, C.double(tolerance))
	if simplified == nil {
		return nil
	}
	return &Geom{simplified}
}

// UnionPolygons tries to merge polygons.
// Returns a single (Multi)Polygon.
// Destroys polygons and returns new allocated (Multi)Polygon as necessary.
func (g *Geos) UnionPolygons(polygons []*Geom) *Geom {
	if len(polygons) == 0 {
		return nil
	}
	if len(polygons) == 1 {
		return polygons[0]
	}
	multiPolygon := g.MultiPolygon(polygons)
	if multiPolygon == nil {
		return nil
	}
	defer g.Destroy(multiPolygon)

	result := C.GEOSUnaryUnion_r(g.v, multiPolygon.v)
	if result == nil {
		return nil
	}
	return &Geom{result}
}

// LineMerge tries to merge lines. Returns slice of LineStrings.
// Destroys lines and returns new allocated LineString Geoms.
func (g *Geos) LineMerge(lines []*Geom) []*Geom {
	if len(lines) <= 1 {
		return lines
	}
	multiLineString := g.MultiLineString(lines)
	if multiLineString == nil {
		return nil
	}
	defer g.Destroy(multiLineString)
	merged := C.GEOSLineMerge_r(g.v, multiLineString.v)
	if merged == nil {
		return nil
	}
	geom := &Geom{merged}
	if g.Type(geom) == "LineString" {
		return []*Geom{geom}
	}

	// extract MultiLineString
	lines = make([]*Geom, 0, g.NumGeoms(geom))
	for _, line := range g.Geoms(geom) {
		lines = append(lines, g.Clone(line))
	}
	g.Destroy(geom)
	return lines
}
