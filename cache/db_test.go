package cache

import (
	"goposm/element"
	"io/ioutil"
	"os"
	"testing"
)

func TestCreateCache(t *testing.T) {
	cache_dir, _ := ioutil.TempDir("", "goposm_test")
	defer os.RemoveAll(cache_dir)

	cache := NewCache(cache_dir)
	defer cache.Close()

	if stat, err := os.Stat(cache_dir); err != nil || !stat.IsDir() {
		t.Error("cache dir not created")
	}
}

func TestReadWriteCoord(t *testing.T) {
	cache_dir, _ := ioutil.TempDir("", "goposm_test")
	defer os.RemoveAll(cache_dir)

	cache := NewCache(cache_dir)
	node := &element.Node{}
	node.Id = 1
	cache.PutCoord(node)
	cache.Close()

	cache = NewCache(cache_dir)
	defer cache.Close()

	data := cache.GetCoord(1)

	if data.Id != 1 {
		t.Errorf("unexpected result of GetNode(1): %v", data)
	}
}

func TestReadWriteNode(t *testing.T) {
	cache_dir, _ := ioutil.TempDir("", "goposm_test")
	defer os.RemoveAll(cache_dir)

	cache := NewCache(cache_dir)
	node := &element.Node{}
	node.Id = 1
	cache.PutNode(node)
	cache.Close()

	cache = NewCache(cache_dir)
	defer cache.Close()

	data := cache.GetNode(1)

	if data.Id != 1 {
		t.Errorf("unexpected result of GetNode(1): %v", data)
	}
}

func TestReadWriteWay(t *testing.T) {
	cache_dir, _ := ioutil.TempDir("", "goposm_test")
	defer os.RemoveAll(cache_dir)

	cache := NewCache(cache_dir)
	way := &element.Way{}
	way.Id = 1
	cache.PutWay(way)
	cache.Close()

	cache = NewCache(cache_dir)
	defer cache.Close()

	data := cache.GetWay(1)

	if data.Id != 1 {
		t.Errorf("unexpected result of GetWay(1): %v", data)
	}
}

func BenchmarkWriteWay(b *testing.B) {
	b.StopTimer()
	cache_dir, _ := ioutil.TempDir("", "goposm_test")
	defer os.RemoveAll(cache_dir)

	cache := NewCache(cache_dir)
	defer cache.Close()

	b.StartTimer()
	way := &element.Way{}
	for i := 0; i < b.N; i++ {
		way.Id = int64(i)
		cache.PutWay(way)
	}
}

func BenchmarkReadWay(b *testing.B) {
	b.StopTimer()
	cache_dir, _ := ioutil.TempDir("", "goposm_test")
	defer os.RemoveAll(cache_dir)

	cache := NewCache(cache_dir)
	defer cache.Close()

	way := &element.Way{}
	for i := 0; i < b.N; i++ {
		way.Id = int64(i)
		cache.PutWay(way)
	}

	b.StartTimer()
	for i := int64(0); i < int64(b.N); i++ {
		if cache.GetWay(i).Id != i {
			b.Fail()
		}
	}

}

func BenchmarkWriteCoord(b *testing.B) {
	b.StopTimer()
	cache_dir, _ := ioutil.TempDir("", "goposm_test")
	defer os.RemoveAll(cache_dir)

	cache := NewCache(cache_dir)
	defer cache.Close()

	b.StartTimer()
	node := &element.Node{}
	for i := 0; i < b.N; i++ {
		node.Id = int64(i)
		cache.PutCoord(node)
	}
}

func BenchmarkReadCoord(b *testing.B) {
	b.StopTimer()
	cache_dir, _ := ioutil.TempDir("", "goposm_test")
	defer os.RemoveAll(cache_dir)

	cache := NewCache(cache_dir)
	defer cache.Close()

	node := &element.Node{}
	for i := 0; i < b.N; i++ {
		node.Id = int64(i)
		cache.PutCoord(node)
	}

	b.StartTimer()
	for i := int64(0); i < int64(b.N); i++ {
		if cache.GetCoord(i).Id != i {
			b.Fail()
		}
	}

}
