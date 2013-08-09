package geos

/*
#cgo LDFLAGS: -lgeos_c
#include "geos_c.h"
#include <stdint.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

extern void goLogString(char *msg);

void debug_wrap(const char *fmt, ...) {
	va_list a_list;
    va_start(a_list, fmt);

	char buf[1024];
	vsnprintf(buf, sizeof(buf), fmt, a_list);
	va_end(a_list);
	goLogString((char *)&buf);
}

void devnull(const char *fmt, ...) {
}

GEOSContextHandle_t initGEOS_r_debug() {
	return initGEOS_r(devnull, debug_wrap);
}

void initGEOS_debug() {
    return initGEOS(devnull, debug_wrap);
}

typedef struct {
    uint32_t num;
    uint32_t *arr;
    uint32_t arrCap;
} queryResult;

void queryResultAppend(queryResult *r, int idx) {
    r->num += 1;
    if (r->num >= r->arrCap) {
        uint32_t newCap = r->arrCap > 0 ? r->arrCap * 2 : 8;
        uint32_t *newArr = malloc(sizeof(uint32_t) * newCap);
        if (r->arrCap == 0) {
            r->arr = newArr;
        } else {
            memcpy(newArr, r->arr, r->num-1);
            free(r->arr);
            r->arr = newArr;
        }
        r->arrCap = newCap;
    }
    r->arr[r->num] = idx;
}

void IndexQueryCallback(void *item, void *userdata) {
    int idx = (size_t)item;
    queryResult *result = (queryResult *)userdata;
    queryResultAppend(result, idx);
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
uint32_t *IndexQuery(
    GEOSContextHandle_t handle,
    GEOSSTRtree *tree,
    const GEOSGeometry *g,
    uint32_t *num)
{
    queryResult result = {0};
    GEOSSTRtree_query_r(handle, tree, g, IndexQueryCallback, &result);
    *num = result.num;
    return result.arr;
}
*/
import "C"
