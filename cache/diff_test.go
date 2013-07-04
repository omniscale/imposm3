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

	cache, err := NewRefIndex(cache_dir, &globalCacheOptions.CoordsIndex)
	if err != nil {
		t.Fatal()
	}

	for w := 0; w < 5; w++ {
		for n := 0; n < 200; n++ {
			cache.addToCache(int64(n), int64(w))
		}
	}
	cache.Close()

	cache, err = NewRefIndex(cache_dir, &globalCacheOptions.CoordsIndex)
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

func BenchmarkWriteDiff(b *testing.B) {
	b.StopTimer()
	cache_dir, _ := ioutil.TempDir("", "goposm_test")
	defer os.RemoveAll(cache_dir)

	cache, err := NewRefIndex(cache_dir, &globalCacheOptions.CoordsIndex)
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
