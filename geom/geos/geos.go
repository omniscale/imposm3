package geos

/*
#cgo LDFLAGS: -lgeos_c
#include "geos_c.h"
#include <stdlib.h>

extern void goDebug(char *msg);
extern void debug_wrap(const char *fmt, ...);
extern GEOSContextHandle_t initGEOS_r_debug();

*/
import "C"

import (
	"fmt"
	"unsafe"
)

//export goDebug
func goDebug(msg *C.char) {
	fmt.Println(C.GoString(msg))
}

type GEOS struct {
	v C.GEOSContextHandle_t
}

type Geom struct {
	v *C.GEOSGeometry
}

type CreateError string
type Error string

func (e Error) Error() string {
	return string(e)
}

func (e CreateError) Error() string {
	return string(e)
}

func NewGEOS() *GEOS {
	geos := &GEOS{}
	geos.v = C.initGEOS_r_debug()
	return geos
}

func (this *GEOS) Finish() {
	if this.v != nil {
		C.finishGEOS_r(this.v)
		this.v = nil
	}
}

type CoordSeq struct {
	v *C.GEOSCoordSequence
}

func (this *GEOS) CreateCoordSeq(size, dim uint32) (*CoordSeq, error) {
	result := C.GEOSCoordSeq_create_r(this.v, C.uint(size), C.uint(dim))
	if result == nil {
		return nil, CreateError("could not create CoordSeq")
	}
	return &CoordSeq{result}, nil
}

func (this *CoordSeq) SetXY(handle *GEOS, i uint32, x, y float64) error {
	if C.GEOSCoordSeq_setX_r(handle.v, this.v, C.uint(i), C.double(x)) == 0 {
		return Error("unable to SetY")
	}
	if C.GEOSCoordSeq_setY_r(handle.v, this.v, C.uint(i), C.double(y)) == 0 {
		return Error("unable to SetX")
	}
	return nil
}

func (this *CoordSeq) AsPoint(handle *GEOS) (*Geom, error) {
	geom := C.GEOSGeom_createPoint_r(handle.v, this.v)
	if geom == nil {
		return nil, CreateError("unable to create Point")
	}
	return &Geom{geom}, nil
}

func (this *CoordSeq) AsLineString(handle *GEOS) (*Geom, error) {
	geom := C.GEOSGeom_createLineString_r(handle.v, this.v)
	if geom == nil {
		return nil, CreateError("unable to create LineString")
	}
	return &Geom{geom}, nil
}

func (this *CoordSeq) AsLinearRing(handle *GEOS) (*Geom, error) {
	ring := C.GEOSGeom_createLinearRing_r(handle.v, this.v)
	if ring == nil {
		return nil, CreateError("unable to create LinearRing")
	}
	return &Geom{ring}, nil
}

func (this *GEOS) CreatePolygon(shell *Geom, holes []*Geom) *Geom {
	if len(holes) > 0 {
		panic("holes not implemented")
	}
	polygon := C.GEOSGeom_createPolygon_r(this.v, shell.v, nil, 0)
	if polygon == nil {
		return nil
	}
	return &Geom{polygon}
}

func (this *GEOS) GeomFromWKT(wkt string) (geom *Geom) {
	wktC := C.CString(wkt)
	defer C.free(unsafe.Pointer(wktC))
	return &Geom{C.GEOSGeomFromWKT_r(this.v, wktC)}
}

func (this *GEOS) Buffer(geom *Geom, size float64) *Geom {
	return &Geom{C.GEOSBuffer_r(this.v, geom.v, C.double(size), 50)}
}

func (this *GEOS) AsWKT(geom *Geom) string {
	return C.GoString(C.GEOSGeomToWKT_r(this.v, geom.v))
}
func (this *GEOS) AsWKB(geom *Geom) ([]byte, error) {
	var size C.size_t
	buf, err := C.GEOSGeomToWKB_buf_r(this.v, geom.v, &size)
	if err != nil {
		return nil, err
	}
	return C.GoBytes(unsafe.Pointer(buf), C.int(size)), nil
}

func (this *GEOS) Area(geom *Geom) float64 {
	var area C.double
	C.GEOSArea_r(this.v, geom.v, &area)
	return float64(area)
}

func (this *GEOS) Destroy(geom *Geom) {
	if geom.v != nil {
		C.GEOSGeom_destroy_r(this.v, geom.v)
		geom.v = nil
	} else {
		panic("double free?")
	}
}
func (this *GEOS) DestroyCoordSeq(coordSeq *CoordSeq) {
	if coordSeq.v != nil {
		C.GEOSCoordSeq_destroy_r(this.v, coordSeq.v)
		coordSeq.v = nil
	} else {
		panic("double free?")
	}
}
