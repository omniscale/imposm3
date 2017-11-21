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

func (this *Geos) FromWkt(wkt string) *Geom {
	wktC := C.CString(wkt)
	defer C.free(unsafe.Pointer(wktC))
	geom := C.GEOSGeomFromWKT_r(this.v, wktC)
	if geom == nil {
		return nil
	}
	return &Geom{geom}
}

func (this *Geos) FromWkb(wkb []byte) *Geom {
	if len(wkb) == 0 {
		return nil
	}
	geom := C.GEOSGeomFromWKB_buf_r(this.v, (*C.uchar)(&wkb[0]), C.size_t(len(wkb)))
	if geom == nil {
		return nil
	}
	return &Geom{geom}
}

func (this *Geos) AsWkt(geom *Geom) string {
	str := C.GEOSGeomToWKT_r(this.v, geom.v)
	if str == nil {
		return ""
	}
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

func (this *Geos) AsEwkbHex(geom *Geom) []byte {
	if this.wkbwriter == nil {
		this.wkbwriter = C.GEOSWKBWriter_create_r(this.v)
		if this.wkbwriter == nil {
			return nil
		}
		if this.srid != 0 {
			C.GEOSWKBWriter_setIncludeSRID_r(this.v, this.wkbwriter, C.char(1))
		}
	}

	if this.srid != 0 {
		C.GEOSSetSRID_r(this.v, geom.v, C.int(this.srid))
	}

	var size C.size_t
	buf := C.GEOSWKBWriter_writeHEX_r(this.v, this.wkbwriter, geom.v, &size)
	if buf == nil {
		return nil
	}
	result := C.GoBytes(unsafe.Pointer(buf), C.int(size))
	C.free(unsafe.Pointer(buf))

	return result

}
