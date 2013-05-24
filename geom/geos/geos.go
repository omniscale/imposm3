package geos

/*
#cgo LDFLAGS: -lgeos_c
#include "geos_c.h"
#include <stdlib.h>

extern void goLogString(char *msg);
extern void debug_wrap(const char *fmt, ...);
extern GEOSContextHandle_t initGEOS_r_debug();
extern void initGEOS_debug();
*/
import "C"

import (
	"goposm/logging"
	"runtime"
	"unsafe"
)

var log = logging.NewLogger("GEOS")

//export goLogString
func goLogString(msg *C.char) {
	log.Printf(C.GoString(msg))
}

type Geos struct {
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

func NewGeos() *Geos {
	geos := &Geos{}
	geos.v = C.initGEOS_r_debug()
	return geos
}

func (this *Geos) Finish() {
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

func (this *Geos) CreateCoordSeq(size, dim uint32) (*CoordSeq, error) {
	result := C.GEOSCoordSeq_create_r(this.v, C.uint(size), C.uint(dim))
	if result == nil {
		return nil, CreateError("could not create CoordSeq")
	}
	return &CoordSeq{result}, nil
}

func (this *CoordSeq) SetXY(handle *Geos, i uint32, x, y float64) error {
	if C.GEOSCoordSeq_setX_r(handle.v, this.v, C.uint(i), C.double(x)) == 0 {
		return Error("unable to SetY")
	}
	if C.GEOSCoordSeq_setY_r(handle.v, this.v, C.uint(i), C.double(y)) == 0 {
		return Error("unable to SetX")
	}
	return nil
}

func (this *CoordSeq) AsPoint(handle *Geos) (*Geom, error) {
	geom := C.GEOSGeom_createPoint_r(handle.v, this.v)
	if geom == nil {
		return nil, CreateError("unable to create Point")
	}
	return &Geom{geom}, nil
}

func (this *CoordSeq) AsLineString(handle *Geos) (*Geom, error) {
	geom := C.GEOSGeom_createLineString_r(handle.v, this.v)
	if geom == nil {
		return nil, CreateError("unable to create LineString")
	}
	return &Geom{geom}, nil
}

func (this *CoordSeq) AsLinearRing(handle *Geos) (*Geom, error) {
	ring := C.GEOSGeom_createLinearRing_r(handle.v, this.v)
	if ring == nil {
		return nil, CreateError("unable to create LinearRing")
	}
	return &Geom{ring}, nil
}

func (this *Geos) CreatePolygon(shell *Geom, holes []*Geom) *Geom {
	if len(holes) > 0 {
		panic("holes not implemented")
	}
	polygon := C.GEOSGeom_createPolygon_r(this.v, shell.v, nil, 0)
	if polygon == nil {
		return nil
	}
	return &Geom{polygon}
}

func (this *Geos) FromWkt(wkt string) (geom *Geom) {
	wktC := C.CString(wkt)
	defer C.free(unsafe.Pointer(wktC))
	return &Geom{C.GEOSGeomFromWKT_r(this.v, wktC)}
}

func (this *Geos) Buffer(geom *Geom, size float64) *Geom {
	return &Geom{C.GEOSBuffer_r(this.v, geom.v, C.double(size), 50)}
}

func (this *Geos) Contains(a, b *Geom) bool {
	result := C.GEOSContains_r(this.v, a.v, b.v)
	if result == 1 {
		return true
	}
	// result == 2 -> exception (already logged to console)
	return false
}

func (this *Geos) ExteriorRing(geom *Geom) *Geom {
	ring := C.GEOSGetExteriorRing_r(this.v, geom.v)
	if ring == nil {
		return nil
	}
	return &Geom{ring}
}

func (this *Geos) Polygon(exterior *Geom, interiors []*Geom) *Geom {
	if len(interiors) == 0 {
		geom := C.GEOSGeom_createPolygon_r(this.v, exterior.v, nil, C.uint(0))
		if geom == nil {
			return nil
		}
		err := C.GEOSNormalize_r(this.v, geom)
		if err != 0 {
			C.GEOSGeom_destroy(geom)
			return nil
		}
		return &Geom{geom}
	}

	interiorPtr := make([]*C.GEOSGeometry, len(interiors))
	for i, geom := range interiors {
		interiorPtr[i] = geom.v
	}
	geom := C.GEOSGeom_createPolygon_r(this.v, exterior.v, &interiorPtr[0], C.uint(len(interiors)))
	if geom == nil {
		return nil
	}
	err := C.GEOSNormalize_r(this.v, geom)
	if err != 0 {
		C.GEOSGeom_destroy(geom)
		return nil
	}
	return &Geom{geom}
}

func (this *Geos) MultiPolygon(polygons []*Geom) *Geom {
	polygonPtr := make([]*C.GEOSGeometry, len(polygons))
	for i, geom := range polygons {
		polygonPtr[i] = geom.v
	}
	geom := C.GEOSGeom_createCollection_r(this.v, C.GEOS_MULTIPOLYGON, &polygonPtr[0], C.uint(len(polygons)))
	if geom == nil {
		return nil
	}
	return &Geom{geom}
}

func (this *Geos) AsWkt(geom *Geom) string {
	str := C.GEOSGeomToWKT_r(this.v, geom.v)
	result := C.GoString(str)
	C.free(unsafe.Pointer(str))
	return result
}
func (this *Geos) AsWkb(geom *Geom) []byte {
	var size C.size_t
	buf := C.GEOSGeomToWKB_buf_r(this.v, geom.v, &size)
	if buf == nil {
		return nil
	}
	result := C.GoBytes(unsafe.Pointer(buf), C.int(size))
	C.free(unsafe.Pointer(buf))
	return result
}

func (this *Geos) FromWkb(wkb []byte) *Geom {
	geom := C.GEOSGeomFromWKB_buf((*C.uchar)(&wkb[0]), C.size_t(len(wkb)))
	if geom == nil {
		return nil
	}
	return &Geom{geom}
}

func (this *Geos) IsValid(geom *Geom) bool {
	if C.GEOSisValid_r(this.v, geom.v) == 1 {
		return true
	}
	return false
}

func (this *Geom) Area() float64 {
	var area C.double
	if ret := C.GEOSArea(this.v, &area); ret == 1 {
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

func (this *Geos) Destroy(geom *Geom) {
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

func (this *Geos) DestroyLater(geom *Geom) {
	runtime.SetFinalizer(geom, destroyGeom)
}

func (this *Geos) DestroyCoordSeq(coordSeq *CoordSeq) {
	if coordSeq.v != nil {
		C.GEOSCoordSeq_destroy_r(this.v, coordSeq.v)
		coordSeq.v = nil
	} else {
		panic("double free?")
	}
}
