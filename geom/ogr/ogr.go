package ogr

/*
#cgo LDFLAGS: -lgdal
#include "ogr_api.h"
#include "cpl_error.h"
#include "cpl_conv.h"
*/
import "C"
import (
	"fmt"
	"imposm3/geom/geos"
	"strings"
	"unsafe"
)

func init() {
	C.OGRRegisterAll()
}

type DataSource struct {
	v C.OGRDataSourceH
}

type Layer struct {
	v C.OGRLayerH
}
type OgrError struct {
	message string
}

func (e *OgrError) Error() string {
	return e.message
}

func lastOgrError(fallback string) error {
	msg := C.CPLGetLastErrorMsg()
	if msg == nil {
		return &OgrError{fallback}
	}
	str := C.GoString(msg)
	if str == "" {
		return &OgrError{fallback}
	}
	return &OgrError{str}
}

func Open(name string) (*DataSource, error) {
	namec := C.CString(name)
	defer C.free(unsafe.Pointer(namec))
	ds := C.OGROpen(namec, 0, nil)
	if ds == nil {
		return nil, lastOgrError("failed to open")
	}
	return &DataSource{ds}, nil
}

func (ds *DataSource) Layer() (*Layer, error) {
	layer := C.OGR_DS_GetLayer(ds.v, 0)
	if layer == nil {
		return nil, lastOgrError("failed to get layer 0")
	}
	return &Layer{layer}, nil
}

func (ds *DataSource) Query(query string) (*Layer, error) {
	// create select query if it is only a where statement
	if !strings.HasPrefix(strings.ToLower(query), "select") {
		layer, err := ds.Layer()
		if err != nil {
			return nil, err
		}
		layerDef := C.OGR_L_GetLayerDefn(layer.v)
		name := C.OGR_FD_GetName(layerDef)
		query = fmt.Sprintf("SELECT * FROM %s WHERE %s", C.GoString(name), query)
	}
	queryc := C.CString(query)
	defer C.free(unsafe.Pointer(queryc))
	layer := C.OGR_DS_ExecuteSQL(ds.v, queryc, nil, nil)
	if layer == nil {
		return nil, lastOgrError("unable to execute query '" + query + "'")
	}
	return &Layer{layer}, nil
}

func (layer *Layer) Wkts() chan string {
	wkts := make(chan string)

	go func() {
		defer close(wkts)

		C.OGR_L_ResetReading(layer.v)
		for {
			feature := C.OGR_L_GetNextFeature(layer.v)
			if feature == nil {
				break
			}
			geom := C.OGR_F_GetGeometryRef(feature)
			var res *C.char
			C.OGR_G_ExportToWkt(geom, &res)
			wkts <- C.GoString(res)
			C.CPLFree(unsafe.Pointer(res))
			C.OGR_F_Destroy(feature)

		}
	}()

	return wkts
}

func (layer *Layer) Wkbs() chan []byte {
	wkbs := make(chan []byte)

	go func() {
		defer close(wkbs)

		C.OGR_L_ResetReading(layer.v)
		for {
			feature := C.OGR_L_GetNextFeature(layer.v)
			if feature == nil {
				break
			}
			geom := C.OGR_F_GetGeometryRef(feature)
			size := C.OGR_G_WkbSize(geom)
			buf := make([]byte, size)
			C.OGR_G_ExportToWkb(geom, C.wkbNDR, (*C.uchar)(&buf[0]))
			wkbs <- buf
			C.OGR_F_Destroy(feature)
		}
	}()
	return wkbs
}

func (layer *Layer) Geoms() chan *geos.Geom {
	geoms := make(chan *geos.Geom)

	go func() {
		defer close(geoms)
		g := geos.NewGeos()
		defer g.Finish()

		wkbs := layer.Wkbs()
		for wkb := range wkbs {
			geom := g.FromWkb(wkb)
			geoms <- geom
		}
	}()
	return geoms
}
