package mapping

import (
	"testing"
)

func TestBool(t *testing.T) {
	match := Match{}
	if false != Bool("", nil, match) {
		t.Fatal()
	}
	if false != Bool("false", nil, match) {
		t.Fatal()
	}
	if false != Bool("no", nil, match) {
		t.Fatal()
	}
	if false != Bool("0", nil, match) {
		t.Fatal()
	}

	if true != Bool("yes", nil, match) {
		t.Fatal()
	}
	if true != Bool("1", nil, match) {
		t.Fatal()
	}
	if true != Bool("true", nil, match) {
		t.Fatal()
	}

	// Bool defaults to true
	if true != Bool("other", nil, match) {
		t.Fatal()
	}

}
