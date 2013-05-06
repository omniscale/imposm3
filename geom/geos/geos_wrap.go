package geos

/*
#cgo LDFLAGS: -lgeos_c
#include "geos_c.h"
#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>

extern void goDebug(char *msg);

void debug_wrap(const char *fmt, ...) {
	va_list a_list;
    va_start(a_list, fmt);

	char buf[100];
	vsnprintf(buf, sizeof(buf), fmt, a_list);
	va_end(a_list);
	goDebug((char *)&buf);
}

GEOSContextHandle_t initGEOS_r_debug() {
	return initGEOS_r(debug_wrap, debug_wrap);
}
*/
import "C"
