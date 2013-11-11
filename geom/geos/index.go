package geos

/*
#cgo LDFLAGS: -lgeos_c
#include "geos_c.h"
#include <stdlib.h>
#include <stdint.h>

extern void IndexQueryCallback(void *, void *);
extern void goIndexSendQueryResult(size_t, void *);
extern uint32_t *IndexQuery(GEOSContextHandle_t, GEOSSTRtree *, const GEOSGeometry *, uint32_t *);
extern void IndexAdd(GEOSContextHandle_t, GEOSSTRtree *, const GEOSGeometry *, size_t);

*/
import "C"
import (
	"sync"
	"unsafe"
)

// IndexGeom is a struct for indexed geometries used by Index
// and returned by IndexQuery.
type IndexGeom struct {
	Geom *Geom
}
type Index struct {
	v     *C.GEOSSTRtree
	mu    *sync.Mutex
	geoms []IndexGeom
}

func (this *Geos) CreateIndex() *Index {
	tree := C.GEOSSTRtree_create_r(this.v, 10)
	if tree == nil {
		panic("unable to create tree")
	}
	return &Index{tree, &sync.Mutex{}, []IndexGeom{}}
}

// IndexQuery adds a geom to the index with the id.
func (this *Geos) IndexAdd(index *Index, geom *Geom) {
	index.mu.Lock()
	defer index.mu.Unlock()
	id := len(index.geoms)
	C.IndexAdd(this.v, index.v, geom.v, C.size_t(id))
	index.geoms = append(index.geoms, IndexGeom{geom})
}

// IndexQuery queries the index for intersections with geom.
func (this *Geos) IndexQuery(index *Index, geom *Geom) []IndexGeom {
	index.mu.Lock()
	defer index.mu.Unlock()
	var num C.uint32_t
	r := C.IndexQuery(this.v, index.v, geom.v, &num)
	if r == nil {
		return nil
	}
	hits := (*[2 << 16]C.uint32_t)(unsafe.Pointer(r))[:num]
	defer C.free(unsafe.Pointer(r))

	var geoms []IndexGeom
	for _, idx := range hits {
		geoms = append(geoms, index.geoms[idx])
	}
	return geoms
}
