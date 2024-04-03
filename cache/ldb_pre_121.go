//go:build ldbpre121

package cache

// #cgo LDFLAGS: -lleveldb
// #include "leveldb/c.h"
import "C"

import (
	"github.com/jmhodges/levigo"
)

func setMaxFileSize(o *levigo.Options, maxFileSize int) {
	// setMaxFileSize is only available with LevelDB 1.21 and higher.
}
