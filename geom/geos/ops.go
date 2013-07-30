package geos

/*
#cgo LDFLAGS: -lgeos_c
#include "geos_c.h"
#include <stdlib.h>
*/
import "C"

func (this *Geos) Contains(a, b *Geom) bool {
	result := C.GEOSContains_r(this.v, a.v, b.v)
	if result == 1 {
		return true
	}
	// result == 2 -> exception (already logged to console)
	return false
}

func (this *Geos) Intersects(a, b *Geom) bool {
	result := C.GEOSIntersects_r(this.v, a.v, b.v)
	if result == 1 {
		return true
	}
	// result == 2 -> exception (already logged to console)
	return false
}

func (this *Geos) Intersection(a, b *Geom) *Geom {
	result := C.GEOSIntersection_r(this.v, a.v, b.v)
	if result == nil {
		return nil
	}
	geom := &Geom{result}
	return geom
}

func (this *Geos) Buffer(geom *Geom, size float64) *Geom {
	buffered := C.GEOSBuffer_r(this.v, geom.v, C.double(size), 50)
	if buffered == nil {
		return nil
	}
	return &Geom{buffered}
}

func (this *Geos) SimplifyPreserveTopology(geom *Geom, tolerance float64) *Geom {
	simplified := C.GEOSTopologyPreserveSimplify_r(this.v, geom.v, C.double(tolerance))
	if simplified == nil {
		return nil
	}
	return &Geom{simplified}
}

// UnionPolygons tries to merge polygons.
// Returns a single (Multi)Polygon.
// Destroys polygons and returns new allocated (Multi)Polygon as necessary.
func (this *Geos) UnionPolygons(polygons []*Geom) *Geom {
	if len(polygons) == 0 {
		return nil
	}
	if len(polygons) == 1 {
		return polygons[0]
	}
	multiPolygon := this.MultiPolygon(polygons)
	if multiPolygon == nil {
		return nil
	}
	defer this.Destroy(multiPolygon)

	result := C.GEOSUnaryUnion_r(this.v, multiPolygon.v)
	if result == nil {
		return nil
	}
	return &Geom{result}
}

// LineMerge tries to merge lines. Returns slice of LineStrings.
// Destroys lines and returns new allocated LineString Geoms.
func (this *Geos) LineMerge(lines []*Geom) []*Geom {
	if len(lines) <= 1 {
		return lines
	}
	multiLineString := this.MultiLineString(lines)
	if multiLineString == nil {
		return nil
	}
	defer this.Destroy(multiLineString)
	merged := C.GEOSLineMerge_r(this.v, multiLineString.v)
	if merged == nil {
		return nil
	}
	geom := &Geom{merged}
	if this.Type(geom) == "LineString" {
		return []*Geom{geom}
	}

	// extract MultiLineString
	lines = make([]*Geom, 0, this.NumGeoms(geom))
	for _, line := range this.Geoms(geom) {
		lines = append(lines, this.Clone(line))
	}
	this.Destroy(geom)
	return lines
}
