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
	if refs[0] != 1 && refs[1] != 10 {
		t.Fatal(refs)
	}

	// insert twice
	refs = insertRefs(refs, 10)
	if refs[0] != 1 && refs[1] != 10 {
		t.Fatal(refs)
	}

	// insert before
	refs = insertRefs(refs, 0)
	if refs[0] != 0 && refs[1] != 1 && refs[2] != 10 {
		t.Fatal(refs)
	}

	// insert after
	refs = insertRefs(refs, 12)
	if refs[0] != 0 && refs[1] != 1 && refs[2] != 10 && refs[3] != 12 {
		t.Fatal(refs)
	}

	// insert between
	refs = insertRefs(refs, 11)
	if refs[0] != 0 && refs[1] != 1 && refs[2] != 10 && refs[3] != 11 && refs[4] != 12 {
		t.Fatal(refs)
	}

}

func TestMarshalRefs(t *testing.T) {
	refs := []int64{1890166659, -1890166659, 0, 1890166, 1890167, 1890167, 1890165}
	buf := MarshalRefs(refs)

	t.Log(len(refs), len(buf))
	result := UnmarshalRefs(buf)

	if len(result) != len(refs) {
		t.Fatal(result)
	}
	for i, ref := range refs {
		if result[i] != ref {
			t.Fatal(result)
		}
	}

}

func TestWriteDiff(t *testing.T) {
	cache_dir, _ := ioutil.TempDir("", "goposm_test")
	defer os.RemoveAll(cache_dir)

	cache, err := NewRefIndex(cache_dir, &osmCacheOptions.CoordsIndex)
	if err != nil {
		t.Fatal()
	}

	for w := 0; w < 5; w++ {
		for n := 0; n < 200; n++ {
			cache.addToCache(int64(n), int64(w))
		}
	}
	cache.Close()

	cache, err = NewRefIndex(cache_dir, &osmCacheOptions.CoordsIndex)
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

func TestMarshalBunch(t *testing.T) {
	bunch := []IdRef{
		{123923123, []int64{1213123}},
		{123923133, []int64{1231237}},
		{123924123, []int64{912412210, 912412213}},
		{123924129, []int64{812412213}},
		{123924130, []int64{91241213}},
		{123924132, []int64{912412210, 9124213, 212412210}},
	}

	buf := MarshalBunch(bunch)
	newBunch := UnmarshalBunch(buf)

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
	bunch := []IdRef{
		{123923123, []int64{1213123}},
		{123923133, []int64{1231237}},
		{123924123, []int64{912412210, 912412213}},
		{123924129, []int64{812412213}},
		{123924130, []int64{91241213}},
		{123924132, []int64{912412210, 9124213, 212412210}},
	}

	for i := 0; i < b.N; i++ {
		buf := MarshalBunch(bunch)
		UnmarshalBunch(buf)
	}
}

func BenchmarkWriteDiff(b *testing.B) {
	b.StopTimer()
	cache_dir, _ := ioutil.TempDir("", "goposm_test")
	defer os.RemoveAll(cache_dir)

	cache, err := NewRefIndex(cache_dir, &osmCacheOptions.CoordsIndex)
	if err != nil {
		b.Fatal()
	}
	defer cache.Close()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for w := 0; w < 5; w++ {
			for n := 0; n < 200; n++ {
				cache.addToCache(int64(n), int64(w))
			}
		}
	}

}

func TestWriteDiffBunch(t *testing.T) {
	cache_dir, _ := ioutil.TempDir("", "goposm_test")
	defer os.RemoveAll(cache_dir)

	cache, err := NewBunchRefCache(cache_dir, &osmCacheOptions.CoordsIndex)
	if err != nil {
		t.Fatal()
	}

	for w := 0; w < 5; w++ {
		for n := 0; n < 200; n++ {
			cache.addToCache(int64(n), int64(w))
		}
	}
	cache.Close()

	cache, err = NewBunchRefCache(cache_dir, &osmCacheOptions.CoordsIndex)
	if err != nil {
		t.Fatal()
	}
	result := cache.Get(100)
	if len(result) != 5 {
		t.Fatal(result)
	}
	cache.Close()

}
