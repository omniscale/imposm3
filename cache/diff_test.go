package cache

import (
	"io/ioutil"
	"os"
	"testing"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/element"
)

func TestDiffCache(t *testing.T) {

	cacheDir, _ := ioutil.TempDir("", "imposm_test")
	defer os.RemoveAll(cacheDir)

	cache, err := newCoordsRefIndex(cacheDir)
	if err != nil {
		t.Fatal()
	}
	defer cache.Close()

	w1 := osm.Way{}
	w1.ID = 100
	w1.Nodes = []osm.Node{
		{Element: osm.Element{ID: 1000}},
		{Element: osm.Element{ID: 1001}},
		{Element: osm.Element{ID: 1002}},
	}
	cache.AddFromWay(&w1)

	w2 := osm.Way{}
	w2.ID = 200
	w2.Nodes = []osm.Node{
		{Element: osm.Element{ID: 1002}},
		{Element: osm.Element{ID: 1003}},
		{Element: osm.Element{ID: 1004}},
	}
	cache.AddFromWay(&w2)

	cache.DeleteFromWay(&w1)

	if ids := cache.Get(1000); len(ids) != 0 {
		t.Fatal(ids)
	}
	if ids := cache.Get(1002); len(ids) != 1 {
		t.Fatal(ids)
	}

}

func TestWriteDiff(t *testing.T) {
	cacheDir, _ := ioutil.TempDir("", "imposm_test")
	defer os.RemoveAll(cacheDir)

	cache, err := newRefIndex(cacheDir, &globalCacheOptions.CoordsIndex)
	if err != nil {
		t.Fatal()
	}
	defer cache.Close()
	cache.SetLinearImport(true)

	for w := 0; w < 5; w++ {
		for n := 0; n < 200; n++ {
			cache.Add(int64(n), int64(w))
		}
	}

	cache.SetLinearImport(false)

	for n := 0; n < 200; n++ {
		refs := cache.Get(int64(n))
		if len(refs) != 5 {
			t.Fatal(refs)
		}
	}
}

func BenchmarkWriteDiff(b *testing.B) {
	b.StopTimer()
	cacheDir, _ := ioutil.TempDir("", "imposm_test")
	defer os.RemoveAll(cacheDir)

	cache, err := newRefIndex(cacheDir, &globalCacheOptions.CoordsIndex)
	if err != nil {
		b.Fatal()
	}
	defer cache.Close()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for w := 0; w < 5; w++ {
			for n := 0; n < 200; n++ {
				cache.Add(int64(n), int64(w))
			}
		}
		cache.Flush()
	}

}

func TestMergeIDRefs(t *testing.T) {
	bunch := []element.IDRefs{}

	bunch = mergeBunch(bunch, []element.IDRefs{element.IDRefs{ID: 50, Refs: []int64{1}}})
	if b := bunch[0]; b.ID != 50 || b.Refs[0] != 1 {
		t.Fatal(bunch)
	}

	// before
	bunch = mergeBunch(bunch, []element.IDRefs{element.IDRefs{ID: 40, Refs: []int64{3}}})
	if b := bunch[0]; b.ID != 40 || b.Refs[0] != 3 {
		t.Fatal(bunch)
	}

	// after
	bunch = mergeBunch(bunch, []element.IDRefs{element.IDRefs{ID: 70, Refs: []int64{4}}})
	if b := bunch[2]; b.ID != 70 || b.Refs[0] != 4 {
		t.Fatal(bunch)
	}

	// in between
	bunch = mergeBunch(bunch, []element.IDRefs{element.IDRefs{ID: 60, Refs: []int64{5}}})
	if b := bunch[2]; b.ID != 60 || b.Refs[0] != 5 {
		t.Fatal(bunch)
	}

	// same (50:1 already inserted)
	bunch = mergeBunch(bunch, []element.IDRefs{element.IDRefs{ID: 50, Refs: []int64{0, 5}}})
	if b := bunch[1]; b.ID != 50 || len(b.Refs) != 3 ||
		b.Refs[0] != 0 || b.Refs[1] != 1 || b.Refs[2] != 5 {
		t.Fatal(bunch)
	}

	if len(bunch) != 4 {
		t.Fatal(bunch)
	}

	// remove multiple
	bunch = mergeBunch(bunch, []element.IDRefs{element.IDRefs{ID: 40, Refs: []int64{}}, element.IDRefs{ID: 60, Refs: []int64{}}})
	if bunch[0].ID != 50 || bunch[1].ID != 70 || len(bunch) != 2 {
		t.Fatal(bunch)
	}

	// add multiple
	bunch = mergeBunch(bunch, []element.IDRefs{element.IDRefs{ID: 40, Refs: []int64{1}}, element.IDRefs{ID: 60, Refs: []int64{1}}, element.IDRefs{ID: 80, Refs: []int64{1}}})
	if len(bunch) != 5 || bunch[0].ID != 40 ||
		bunch[2].ID != 60 || bunch[4].ID != 80 {
		t.Fatal(bunch)
	}

}

func TestIDRefBunches(t *testing.T) {
	bunches := make(idRefBunches)
	bunches.add(1, 100, 999)

	if r := bunches[1].idRefs[0]; r.ID != 100 || r.Refs[0] != 999 {
		t.Fatal(bunches)
	}

	// before
	bunches.add(1, 99, 888)
	if r := bunches[1].idRefs[0]; r.ID != 99 || r.Refs[0] != 888 {
		t.Fatal(bunches)
	}

	// after
	bunches.add(1, 102, 777)
	if r := bunches[1].idRefs[2]; r.ID != 102 || r.Refs[0] != 777 {
		t.Fatal(bunches)
	}

	// in between
	bunches.add(1, 101, 666)
	if r := bunches[1].idRefs[2]; r.ID != 101 || r.Refs[0] != 666 {
		t.Fatal(bunches)
	}

	// same id
	bunches.add(1, 100, 998)
	if r := bunches[1].idRefs[1]; r.ID != 100 || r.Refs[0] != 998 || r.Refs[1] != 999 {
		t.Fatal(bunches)
	}

	// duplicate with same id and same ref
	bunches.add(1, 100, 998)
	if r := bunches[1].idRefs[1]; r.ID != 100 || r.Refs[0] != 998 || r.Refs[1] != 999 {
		t.Fatal(bunches)
	}

	if len(bunches) != 1 {
		t.Fatal(bunches)
	}
	if bunches[1].id != 1 {
		t.Fatal(bunches)
	}
	if len(bunches[1].idRefs) != 4 {
		t.Fatal(bunches)
	}
}
