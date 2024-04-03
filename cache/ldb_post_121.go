//go:build !ldbpre121

package cache

// #cgo LDFLAGS: -lleveldb
// #include "leveldb/c.h"
import "C"

import (
	"unsafe"

	"github.com/jmhodges/levigo"
)

func setMaxFileSize(o *levigo.Options, maxFileSize int) {
	p := (*C.struct_leveldb_options_t)(unsafe.Pointer(o.Opt))
	C.leveldb_options_set_max_file_size(p, C.size_t(maxFileSize))
}
