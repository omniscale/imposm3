package geos

/*
#cgo LDFLAGS: -lgeos_c
#include "geos_c.h"
#include <stdlib.h>
*/
import "C"

import (
	"unsafe"
)

func (g *Geos) FromWkt(wkt string) *Geom {
	wktC := C.CString(wkt)
	defer C.free(unsafe.Pointer(wktC))
	geom := C.GEOSGeomFromWKT_r(g.v, wktC)
	if geom == nil {
		return nil
	}
	return &Geom{geom}
}

func (g *Geos) FromWkb(wkb []byte) *Geom {
	if len(wkb) == 0 {
		return nil
	}
	geom := C.GEOSGeomFromWKB_buf_r(g.v, (*C.uchar)(&wkb[0]), C.size_t(len(wkb)))
	if geom == nil {
		return nil
	}
	return &Geom{geom}
}

func (g *Geos) AsWkt(geom *Geom) string {
	str := C.GEOSGeomToWKT_r(g.v, geom.v)
	if str == nil {
		return ""
	}
	result := C.GoString(str)
	C.free(unsafe.Pointer(str))
	return result
}

func (g *Geos) AsWkb(geom *Geom) []byte {
	var size C.size_t
	buf := C.GEOSGeomToWKB_buf_r(g.v, geom.v, &size)
	if buf == nil {
		return nil
	}
	result := C.GoBytes(unsafe.Pointer(buf), C.int(size))
	C.free(unsafe.Pointer(buf))
	return result
}

func (g *Geos) AsEwkbHex(geom *Geom) []byte {
	if g.wkbwriter == nil {
		g.wkbwriter = C.GEOSWKBWriter_create_r(g.v)
		if g.wkbwriter == nil {
			return nil
		}
		if g.srid != 0 {
			C.GEOSWKBWriter_setIncludeSRID_r(g.v, g.wkbwriter, C.char(1))
		}
	}

	if g.srid != 0 {
		C.GEOSSetSRID_r(g.v, geom.v, C.int(g.srid))
	}

	var size C.size_t
	buf := C.GEOSWKBWriter_writeHEX_r(g.v, g.wkbwriter, geom.v, &size)
	if buf == nil {
		return nil
	}
	result := C.GoBytes(unsafe.Pointer(buf), C.int(size))
	C.free(unsafe.Pointer(buf))

	return result

}
