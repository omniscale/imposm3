package geos

import "testing"
import "math"

func TestFoo(t *testing.T) {
	if x := Foo(); math.Abs(x-3.1415) > 0.01 {
		t.Errorf("Buffer is not 3.1415: %f", x)
	}
	geos := NewGEOS()
	BufferBench(geos)
}
