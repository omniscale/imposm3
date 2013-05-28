package geos

/*
#cgo LDFLAGS: -lgeos_c
#include "geos_c.h"
#include <stdlib.h>

extern void IndexQuerySendCallback(void *, void *);
extern void goIndexSendQueryResult(size_t, void *);
extern void IndexQuery(GEOSContextHandle_t, GEOSSTRtree *, const GEOSGeometry *, void *);
extern void IndexAdd(GEOSContextHandle_t, GEOSSTRtree *, const GEOSGeometry *, size_t);

*/
import "C"
import (
	"sync"
	"unsafe"
)

// IndexGeom is a struct for indexed geometries used by Index
// and returned by IndexQuery. Access to Prepared requires acquiring .Lock()
type IndexGeom struct {
	*sync.Mutex // Mutex for Prepared
	Geom        *Geom
	Prepared    *PreparedGeom
}
type Index struct {
	v     *C.GEOSSTRtree
	geoms []IndexGeom
}

func (this *Geos) CreateIndex() *Index {
	tree := C.GEOSSTRtree_create_r(this.v, 10)
	if tree == nil {
		panic("unable to create tree")
	}
	return &Index{tree, []IndexGeom{}}
}

// IndexQuery adds a geom to the index with the id.
func (this *Geos) IndexAdd(index *Index, geom *Geom) {
	id := len(index.geoms)
	C.IndexAdd(this.v, index.v, geom.v, C.size_t(id))
	prep := this.Prepare(geom)
	index.geoms = append(index.geoms, IndexGeom{&sync.Mutex{}, geom, prep})
}

// IndexQuery queries the index for intersections with geom.
func (this *Geos) IndexQuery(index *Index, geom *Geom) []IndexGeom {
	hits := make(chan int)
	go func() {
		// using a pointer to our hits chan to pass it through
		// C.IndexQuerySendCallback (in C.IndexQuery) back
		// to goIndexSendQueryResult
		C.IndexQuery(this.v, index.v, geom.v, unsafe.Pointer(&hits))
		close(hits)
	}()
	var geoms []IndexGeom
	for idx := range hits {
		geoms = append(geoms, index.geoms[idx])
	}
	return geoms
}

//export goIndexSendQueryResult
func goIndexSendQueryResult(id C.size_t, ptr unsafe.Pointer) {
	results := *(*chan int)(ptr)
	results <- int(id)
}
