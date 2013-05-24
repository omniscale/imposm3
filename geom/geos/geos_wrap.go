package geos

/*
#cgo LDFLAGS: -lgeos_c
#include "geos_c.h"
#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>

extern void goLogString(char *msg);
extern void goSendQueryResult(size_t, void *);

void debug_wrap(const char *fmt, ...) {
	va_list a_list;
    va_start(a_list, fmt);

	char buf[100];
	vsnprintf(buf, sizeof(buf), fmt, a_list);
	va_end(a_list);
	goLogString((char *)&buf);
}

GEOSContextHandle_t initGEOS_r_debug() {
	return initGEOS_r(debug_wrap, debug_wrap);
}

void initGEOS_debug() {
    return initGEOS(debug_wrap, debug_wrap);
}

// wrap goIndexSendQueryResult
void IndexQuerySendCallback(void *item, void *userdata) {
    goIndexSendQueryResult((size_t)item, userdata);
}

void IndexAdd(
    GEOSContextHandle_t handle,
    GEOSSTRtree *tree,
    const GEOSGeometry *g,
    size_t id)
{
    // instead of storing a void *, we just store our id
    // this is safe since GEOS doesn't access the item pointer
    GEOSSTRtree_insert_r(handle, tree, g, (void *)id);
}

// query with our custom callback
void IndexQuery(
    GEOSContextHandle_t handle,
    GEOSSTRtree *tree,
    const GEOSGeometry *g,
    void *userdata)
{
    GEOSSTRtree_query_r(handle, tree, g, IndexQuerySendCallback, userdata);
    }
*/
import "C"
