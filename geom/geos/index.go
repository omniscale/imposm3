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

func (g *Geos) CreateIndex() *Index {
	tree := C.GEOSSTRtree_create_r(g.v, 10)
	if tree == nil {
		panic("unable to create tree")
	}
	return &Index{tree, &sync.Mutex{}, []IndexGeom{}}
}

// IndexAdd adds a geom to the index with the id.
func (g *Geos) IndexAdd(index *Index, geom *Geom) {
	index.mu.Lock()
	defer index.mu.Unlock()
	id := len(index.geoms)
	C.IndexAdd(g.v, index.v, geom.v, C.size_t(id))
	index.geoms = append(index.geoms, IndexGeom{geom})
}

// IndexQueryGeoms queries the index for intersections with geom.
func (g *Geos) IndexQueryGeoms(index *Index, geom *Geom) []IndexGeom {
	hits := g.IndexQuery(index, geom)

	var geoms []IndexGeom
	for _, idx := range hits {
		geoms = append(geoms, index.geoms[idx])
	}
	return geoms
}

// IndexQuery queries the index for intersections with geom.
func (g *Geos) IndexQuery(index *Index, geom *Geom) []int {
	index.mu.Lock()
	defer index.mu.Unlock()
	var num C.uint32_t
	r := C.IndexQuery(g.v, index.v, geom.v, &num)
	if r == nil {
		return nil
	}
	hits := (*[2 << 16]C.uint32_t)(unsafe.Pointer(r))[:num]
	defer C.free(unsafe.Pointer(r))

	indices := make([]int, len(hits))
	for i := range hits {
		indices[i] = int(hits[i])
	}
	return indices
}
