package cache

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestInsertRefs(t *testing.T) {

	refs := make([]int64, 0, 1)

	refs = insertRefs(refs, 1)
	if refs[0] != 1 {
		t.Fatal(refs)
	}

	refs = insertRefs(refs, 10)
	if refs[0] != 1 || refs[1] != 10 {
		t.Fatal(refs)
	}

	// insert twice
	refs = insertRefs(refs, 10)
	if refs[0] != 1 || refs[1] != 10 || len(refs) != 2 {
		t.Fatal(refs)
	}

	// insert before
	refs = insertRefs(refs, 0)
	if refs[0] != 0 || refs[1] != 1 || refs[2] != 10 {
		t.Fatal(refs)
	}

	// insert after
	refs = insertRefs(refs, 12)
	if refs[0] != 0 || refs[1] != 1 || refs[2] != 10 || refs[3] != 12 {
		t.Fatal(refs)
	}

	// insert between
	refs = insertRefs(refs, 11)
	if refs[0] != 0 || refs[1] != 1 || refs[2] != 10 || refs[3] != 11 || refs[4] != 12 {
		t.Fatal(refs)
	}

}

func TestWriteDiff(t *testing.T) {
	cache_dir, _ := ioutil.TempDir("", "goposm_test")
	defer os.RemoveAll(cache_dir)

	cache, err := newRefIndex(cache_dir, &globalCacheOptions.CoordsIndex)
	if err != nil {
		t.Fatal()
	}

	for w := 0; w < 5; w++ {
		for n := 0; n < 200; n++ {
			cache.add <- idRef{id: int64(n), ref: int64(w)}
		}
	}
	cache.Close()

	cache, err = newRefIndex(cache_dir, &globalCacheOptions.CoordsIndex)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	for n := 0; n < 200; n++ {
		refs := cache.Get(int64(n))
		if len(refs) != 5 {
			t.Fatal(refs)
		}
	}
}

func TestmarshalBunch(t *testing.T) {
	bunch := []idRefs{
		{123923123, []int64{1213123}},
		{123923133, []int64{1231237}},
		{123924123, []int64{912412210, 912412213}},
		{123924129, []int64{812412213}},
		{123924130, []int64{91241213}},
		{123924132, []int64{912412210, 9124213, 212412210}},
	}

	buf := marshalBunch(bunch)
	newBunch := unmarshalBunch(buf)

	t.Log(len(buf), float64(len(buf))/6.0)

	if len(newBunch) != 6 {
		t.Fatal(newBunch)
	}
	if newBunch[0].id != 123923123 || newBunch[0].refs[0] != 1213123 {
		t.Fatal(newBunch[0])
	}
	if newBunch[1].id != 123923133 || newBunch[1].refs[0] != 1231237 {
		t.Fatal(newBunch[1])
	}
	if newBunch[2].id != 123924123 || newBunch[2].refs[0] != 912412210 || newBunch[2].refs[1] != 912412213 {
		t.Fatal(newBunch[2])
	}
	if newBunch[5].id != 123924132 || newBunch[5].refs[2] != 212412210 {
		t.Fatal(newBunch[5])
	}
}

func BenchmarkMarshalBunch(b *testing.B) {
	bunch := []idRefs{
		{123923123, []int64{1213123}},
		{123923133, []int64{1231237}},
		{123924123, []int64{912412210, 912412213}},
		{123924129, []int64{812412213}},
		{123924130, []int64{91241213}},
		{123924132, []int64{912412210, 9124213, 212412210}},
	}

	for i := 0; i < b.N; i++ {
		buf := marshalBunch(bunch)
		unmarshalBunch(buf)
	}
}

func BenchmarkWriteDiff(b *testing.B) {
	b.StopTimer()
	cache_dir, _ := ioutil.TempDir("", "goposm_test")
	defer os.RemoveAll(cache_dir)

	cache, err := newRefIndex(cache_dir, &globalCacheOptions.CoordsIndex)
	if err != nil {
		b.Fatal()
	}
	defer cache.Close()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for w := 0; w < 5; w++ {
			for n := 0; n < 200; n++ {
				cache.add <- idRef{id: int64(n), ref: int64(w)}
			}
		}
	}

}

func TestMergeIdRefs(t *testing.T) {
	bunch := []idRefs{}

	bunch = mergeBunch(bunch, []idRefs{idRefs{50, []int64{1}}})
	if b := bunch[0]; b.id != 50 || b.refs[0] != 1 {
		t.Fatal(bunch)
	}

	// before
	bunch = mergeBunch(bunch, []idRefs{idRefs{40, []int64{3}}})
	if b := bunch[0]; b.id != 40 || b.refs[0] != 3 {
		t.Fatal(bunch)
	}

	// after
	bunch = mergeBunch(bunch, []idRefs{idRefs{70, []int64{4}}})
	if b := bunch[2]; b.id != 70 || b.refs[0] != 4 {
		t.Fatal(bunch)
	}

	// in between
	bunch = mergeBunch(bunch, []idRefs{idRefs{60, []int64{5}}})
	if b := bunch[2]; b.id != 60 || b.refs[0] != 5 {
		t.Fatal(bunch)
	}

	// same (50:1 already inserted)
	bunch = mergeBunch(bunch, []idRefs{idRefs{50, []int64{0, 5}}})
	if b := bunch[1]; b.id != 50 || len(b.refs) != 3 ||
		b.refs[0] != 0 || b.refs[1] != 1 || b.refs[2] != 5 {
		t.Fatal(bunch)
	}

	if len(bunch) != 4 {
		t.Fatal(bunch)
	}

	// remove multiple
	bunch = mergeBunch(bunch, []idRefs{idRefs{40, []int64{}}, idRefs{60, []int64{}}})
	if bunch[0].id != 50 || bunch[1].id != 70 || len(bunch) != 2 {
		t.Fatal(bunch)
	}

	// add multiple
	bunch = mergeBunch(bunch, []idRefs{idRefs{40, []int64{1}}, idRefs{60, []int64{1}}, idRefs{80, []int64{1}}})
	if len(bunch) != 5 || bunch[0].id != 40 ||
		bunch[2].id != 60 || bunch[4].id != 80 {
		t.Fatal(bunch)
	}

}

func TestIdRefBunches(t *testing.T) {
	bunches := make(idRefBunches)
	bunches.add(1, 100, 999)

	if r := bunches[1].idRefs[0]; r.id != 100 || r.refs[0] != 999 {
		t.Fatal(bunches)
	}

	// before
	bunches.add(1, 99, 888)
	if r := bunches[1].idRefs[0]; r.id != 99 || r.refs[0] != 888 {
		t.Fatal(bunches)
	}

	// after
	bunches.add(1, 102, 777)
	if r := bunches[1].idRefs[2]; r.id != 102 || r.refs[0] != 777 {
		t.Fatal(bunches)
	}

	// in between
	bunches.add(1, 101, 666)
	if r := bunches[1].idRefs[2]; r.id != 101 || r.refs[0] != 666 {
		t.Fatal(bunches)
	}

	// same id
	bunches.add(1, 100, 998)
	if r := bunches[1].idRefs[1]; r.id != 100 || r.refs[0] != 998 || r.refs[1] != 999 {
		t.Fatal(bunches)
	}

	// duplicate with same id and same ref
	bunches.add(1, 100, 998)
	if r := bunches[1].idRefs[1]; r.id != 100 || r.refs[0] != 998 || r.refs[1] != 999 {
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
