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

func TestReadWriteNode(t *testing.T) {
	cache_dir, _ := ioutil.TempDir("", "goposm_test")
	defer os.RemoveAll(cache_dir)

	cache := NewCache(cache_dir)
	node := &element.Node{}
	node.Id = 1
	cache.PutCoord(node)
	cache.Close()

	cache = NewCache(cache_dir)
	defer cache.Close()

	data := cache.GetCoord(element.OSMID(1))

	if data.Id != 1 {
		t.Errorf("unexpected result of GetNode(1): %v", data)
	}
}
