package geos

/*
#cgo LDFLAGS: -lgeos_c
#include "geos_c.h"
#include <stdlib.h>

extern void goDebug(char *msg);
extern void debug_wrap(const char *fmt, ...);
extern GEOSContextHandle_t initGEOS_r_debug();
extern void initGEOS_debug();
*/
import "C"

import (
	"fmt"
	"runtime"
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

func init() {
	/*
		Init global GEOS handle for non _r calls.
		In theory we need to always call the _r functions
		with a thread/goroutine-local GEOS instance to get thread
		safe behaviour. Some functions don't need a GEOS instance though
		and we can make use of that e.g. to call GEOSGeom_destroy in
		finalizer.
	*/
	C.initGEOS_debug()
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

func (this *GEOS) IsValid(geom *Geom) bool {
	if C.GEOSisValid_r(this.v, geom.v) == 1 {
		return true
	}
	return false
}

func (this *Geom) Area() float64 {
	var area C.double
	if ret := C.GEOSArea(this.v, &area); ret == 0 {
		return float64(area)
	} else {
		return 0
	}
}

func (this *Geom) Bounds() *Bounds {
	geom := C.GEOSEnvelope(this.v)
	if geom == nil {
		return nil
	}
	extRing := C.GEOSGetExteriorRing(geom)
	if extRing == nil {
		return nil
	}
	cs := C.GEOSGeom_getCoordSeq(extRing)
	var csLen C.uint
	C.GEOSCoordSeq_getSize(cs, &csLen)
	minx := 1.e+20
	maxx := -1e+20
	miny := 1.e+20
	maxy := -1e+20
	var temp C.double
	for i := 0; i < int(csLen); i++ {
		C.GEOSCoordSeq_getX(cs, C.uint(i), &temp)
		x := float64(temp)
		if x < minx {
			minx = x
		}
		if x > maxx {
			maxx = x
		}
		C.GEOSCoordSeq_getY(cs, C.uint(i), &temp)
		y := float64(temp)
		if y < miny {
			miny = y
		}
		if y > maxy {
			maxy = y
		}
	}

	return &Bounds{minx, miny, maxx, maxy}
}

type Bounds struct {
	MinX float64
	MinY float64
	MaxX float64
	MaxY float64
}

func (this *GEOS) Destroy(geom *Geom) {
	if geom.v != nil {
		C.GEOSGeom_destroy_r(this.v, geom.v)
		geom.v = nil
	} else {
		panic("double free?")
	}
}

func destroyGeom(geom *Geom) {
	C.GEOSGeom_destroy(geom.v)
}

func (this *GEOS) DestroyLater(geom *Geom) {
	runtime.SetFinalizer(geom, destroyGeom)
}

func (this *GEOS) DestroyCoordSeq(coordSeq *CoordSeq) {
	if coordSeq.v != nil {
		C.GEOSCoordSeq_destroy_r(this.v, coordSeq.v)
		coordSeq.v = nil
	} else {
		panic("double free?")
	}
}
