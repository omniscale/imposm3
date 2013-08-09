package geos

import (
	"fmt"

	"testing"
)

func TestFoo(t *testing.T) {
	_ = NewGeos()
}

func BenchmarkWKB(b *testing.B) {
	g := NewGeos()
	defer g.Finish()

	for i := 0; i < b.N; i++ {
		cs, _ := g.CreateCoordSeq(10, 2)
		for i := 0; i < 10; i++ {
			cs.SetXY(g, uint32(i), float64(i*10), 0)
		}
		geom, _ := cs.AsLineString(g)
		if g.IsValid(geom) == false {
			b.Fail()
		}
		g.AsWkb(geom)
		g.Destroy(geom)
	}
}

func TestIndexQuery(t *testing.T) {
	g := NewGeos()
	defer g.Finish()

	idx := g.CreateIndex()

	for i := 0; i < 10; i++ {
		p := g.FromWkt(fmt.Sprintf("POLYGON((%d 0, 10 0, 10 10, %d 10, %d 0))", i, i, i))
		if p == nil {
			t.Fatal()
		}
		g.IndexAdd(idx, p)
	}

	if geoms := g.IndexQuery(idx, g.Point(0, 10.000001)); len(geoms) != 0 {
		t.Fatal(geoms)
	}

	if geoms := g.IndexQuery(idx, g.Point(9.5, 5)); len(geoms) != 10 {
		t.Fatal(geoms)
	}

	if geoms := g.IndexQuery(idx, g.Point(0.5, 5)); len(geoms) != 1 {
		t.Fatal(geoms)
	}
	if geoms := g.IndexQuery(idx, g.Point(4.5, 5)); len(geoms) != 5 {
		t.Fatal(geoms)
	}

}

func BenchmarkIndexQuery(b *testing.B) {
	g := NewGeos()
	defer g.Finish()

	idx := g.CreateIndex()
	for i := 0; i < 10; i++ {
		p := g.FromWkt(fmt.Sprintf("POLYGON((%d 0, 10 0, 10 10, %d 10, %d 0))", i, i, i))
		if p == nil {
			b.Fatal()
		}
		g.IndexAdd(idx, p)
	}

	for i := 0; i < b.N; i++ {
		if geoms := g.IndexQuery(idx, g.Point(8.5, 5)); len(geoms) != 9 {
			b.Fatal(geoms)
		}
	}

	// if geoms := g.IndexQuery(idx, g.Point(0, 0)); len(geoms) != 10 {
	// 	b.Fatal(geoms)
	// }

	// if geoms := g.IndexQuery(idx, g.Point(5, 5)); len(geoms) != 10 {
	// 	b.Fatal(geoms)
	// }

}
