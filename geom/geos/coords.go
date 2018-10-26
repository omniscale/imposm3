package geos

/*
#cgo LDFLAGS: -lgeos_c
#include "geos_c.h"
#include <stdlib.h>
*/
import "C"

type CoordSeq struct {
	v *C.GEOSCoordSequence
}

func (g *Geos) CreateCoordSeq(size, dim uint32) (*CoordSeq, error) {
	result := C.GEOSCoordSeq_create_r(g.v, C.uint(size), C.uint(dim))
	if result == nil {
		return nil, CreateError("could not create CoordSeq")
	}
	return &CoordSeq{result}, nil
}

func (g *CoordSeq) SetXY(handle *Geos, i uint32, x, y float64) error {
	if C.GEOSCoordSeq_setX_r(handle.v, g.v, C.uint(i), C.double(x)) == 0 {
		return Error("unable to SetY")
	}
	if C.GEOSCoordSeq_setY_r(handle.v, g.v, C.uint(i), C.double(y)) == 0 {
		return Error("unable to SetX")
	}
	return nil
}

func (g *CoordSeq) AsPoint(handle *Geos) (*Geom, error) {
	geom := C.GEOSGeom_createPoint_r(handle.v, g.v)
	if geom == nil {
		return nil, CreateError("unable to create Point")
	}
	return &Geom{geom}, nil
}

func (g *CoordSeq) AsLineString(handle *Geos) (*Geom, error) {
	geom := C.GEOSGeom_createLineString_r(handle.v, g.v)
	if geom == nil {
		return nil, CreateError("unable to create LineString")
	}
	return &Geom{geom}, nil
}

func (g *CoordSeq) AsLinearRing(handle *Geos) (*Geom, error) {
	ring := C.GEOSGeom_createLinearRing_r(handle.v, g.v)
	if ring == nil {
		return nil, CreateError("unable to create LinearRing")
	}
	return &Geom{ring}, nil
}

func (g *Geos) DestroyCoordSeq(coordSeq *CoordSeq) {
	if coordSeq.v != nil {
		C.GEOSCoordSeq_destroy_r(g.v, coordSeq.v)
		coordSeq.v = nil
	} else {
		panic("double free?")
	}
}
