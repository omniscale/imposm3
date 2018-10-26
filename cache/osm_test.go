package cache

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	osm "github.com/omniscale/go-osm"
)

func TestCreateCache(t *testing.T) {
	cache_dir, _ := ioutil.TempDir("", "imposm_test")
	defer os.RemoveAll(cache_dir)

	cache, err := newNodesCache(cache_dir)
	if err != nil {
		t.Fatal()
	}
	defer cache.Close()

	if stat, err := os.Stat(cache_dir); err != nil || !stat.IsDir() {
		t.Error("cache dir not created")
	}
}

func TestReadWriteNode(t *testing.T) {
	cache_dir, _ := ioutil.TempDir("", "imposm_test")
	defer os.RemoveAll(cache_dir)

	cache, err := newNodesCache(cache_dir)
	if err != nil {
		t.Fatal()
	}
	node := &osm.Node{
		OSMElem: osm.OSMElem{
			ID:   1234,
			Tags: osm.Tags{"foo": "bar"}},
	}
	cache.PutNode(node)
	cache.Close()

	cache, err = newNodesCache(cache_dir)
	if err != nil {
		t.Fatal()
	}
	defer cache.Close()

	data, err := cache.GetNode(1234)
	if data.ID != 1234 || data.Tags["foo"] != "bar" {
		t.Errorf("unexpected result of GetNode: %v", data)
	}

	data, err = cache.GetNode(99)
	if data != nil {
		t.Error("missing node not nil")
	}

}

func TestReadWriteWay(t *testing.T) {
	cache_dir, _ := ioutil.TempDir("", "imposm_test")
	defer os.RemoveAll(cache_dir)

	cache, err := newWaysCache(cache_dir)
	if err != nil {
		t.Fatal()
	}
	way := &osm.Way{
		OSMElem: osm.OSMElem{
			ID:   1234,
			Tags: osm.Tags{"foo": "bar"}},
		Refs: []int64{942374923, 23948234},
	}
	cache.PutWay(way)
	cache.Close()

	cache, err = newWaysCache(cache_dir)
	if err != nil {
		t.Fatal()
	}
	defer cache.Close()

	data, _ := cache.GetWay(1234)

	if data.ID != 1234 || data.Tags["foo"] != "bar" {
		t.Errorf("unexpected result of GetWay: %#v", data)
	}
	if len(data.Refs) != 2 ||
		data.Refs[0] != 942374923 ||
		data.Refs[1] != 23948234 {
		t.Errorf("unexpected result of GetWay: %#v", data)
	}
}

func TestReadMissingWay(t *testing.T) {
	cache_dir, _ := ioutil.TempDir("", "imposm_test")
	defer os.RemoveAll(cache_dir)

	cache, err := newWaysCache(cache_dir)
	if err != nil {
		t.Fatal()
	}
	defer cache.Close()

	data, _ := cache.GetWay(1234)

	if data != nil {
		t.Errorf("missing way did not return nil")
	}
}

func BenchmarkWriteWay(b *testing.B) {
	b.StopTimer()
	cache_dir, _ := ioutil.TempDir("", "imposm_test")
	defer os.RemoveAll(cache_dir)

	cache, err := newWaysCache(cache_dir)
	if err != nil {
		b.Fatal()
	}
	defer cache.Close()

	b.StartTimer()
	way := &osm.Way{
		OSMElem: osm.OSMElem{Tags: osm.Tags{"foo": "bar"}},
		Refs:    []int64{942374923, 23948234},
	}
	for i := 0; i < b.N; i++ {
		way.ID = int64(i)
		cache.PutWay(way)
	}
}

func BenchmarkReadWay(b *testing.B) {
	b.StopTimer()
	cache_dir, _ := ioutil.TempDir("", "imposm_test")
	defer os.RemoveAll(cache_dir)

	cache, err := newWaysCache(cache_dir)
	if err != nil {
		b.Fatal()
	}
	defer cache.Close()

	way := &osm.Way{}
	for i := 0; i < b.N; i++ {
		way.ID = int64(i)
		cache.PutWay(way)
	}

	b.StartTimer()
	for i := int64(0); i < int64(b.N); i++ {
		if coord, err := cache.GetWay(i); err != nil || coord.ID != i {
			b.Fail()
		}
	}

}

func TestIDs(t *testing.T) {
	for i := 0; i < 10000; i++ {
		id := rand.Int63()
		if idFromKeyBuf(idToKeyBuf(id)) != id {
			t.Fatal()
		}
	}

	// check that id buffers are in lexical order
	var id = int64(0)
	var prevKey string
	for i := 0; i < 100; i++ {
		id += rand.Int63n(1e12)
		buf := idToKeyBuf(id)
		if prevKey > string(buf) {
			t.Fatal()
		}
		prevKey = string(buf)
	}

}
