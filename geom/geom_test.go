package geom

import (
	"goposm/element"
	"regexp"
	"testing"
)

func _TestLineString(t *testing.T) {
	nodes := make([]element.Node, 2)
	nodes[0] = element.Node{Lat: 0, Long: 0}
	nodes[1] = element.Node{Lat: 0, Long: 10}
	wkt := LineString(nodes)
	re := regexp.MustCompile("LINESTRING \\(0\\.0* 0\\.0*, 10\\.0* 0\\.0*\\)")
	if !re.Match(wkt) {
		t.Errorf("%#v", wkt)
	}
}

func BenchmarkLineString(b *testing.B) {
	size := 16
	nodes := make([]element.Node, size)
	for i := 0; i < size; i++ {
		nodes[i] = element.Node{Lat: 0, Long: float64(i)}
	}

	for i := 0; i < b.N; i++ {
		LineString(nodes)
	}
}
