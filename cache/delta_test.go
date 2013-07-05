package cache

import (
	"goposm/element"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"testing"
)

func mknode(id int64) element.Node {
	return element.Node{
		OSMElem: element.OSMElem{
			Id: id,
		},
		Long: 8,
		Lat:  10,
	}
}

func TestReadWriteDeltaCoords(t *testing.T) {
	cache_dir, _ := ioutil.TempDir("", "goposm_test")
	defer os.RemoveAll(cache_dir)

	cache, err := newDeltaCoordsCache(cache_dir)
	if err != nil {
		t.Fatal()
	}

	// create list with nodes from Id 0->999 in random order
	nodeIds := rand.Perm(1000)
	nodes := make([]element.Node, 1000)
	for i := 0; i < len(nodes); i++ {
		nodes[i] = mknode(int64(nodeIds[i]))
	}

	// add nodes in batches of ten
	for i := 0; i <= len(nodes)-10; i = i + 10 {
		// sort each batch as required by PutCoords
		sort.Sort(byId(nodes[i : i+10]))
		cache.PutCoords(nodes[i : i+10])
	}

	cache.Close()

	cache, err = newDeltaCoordsCache(cache_dir)
	if err != nil {
		t.Fatal()
	}

	for i := 0; i < len(nodes); i++ {
		data, err := cache.GetCoord(int64(i))
		if err == NotFound {
			t.Fatal("missing coord:", i)
		} else if err != nil {
			t.Fatal(err)
		}
		if data.Id != int64(i) {
			t.Errorf("unexpected result of GetNode: %v", data)
		}
	}

	_, err = cache.GetCoord(999999)
	if err != NotFound {
		t.Error("missing node returned not NotFound")
	}

	// test delete
	cache.PutCoords([]element.Node{mknode(999999)})
	cache.Close()

	cache, err = newDeltaCoordsCache(cache_dir)
	if err != nil {
		t.Fatal()
	}

	_, err = cache.GetCoord(999999)
	if err == NotFound {
		t.Error("missing coord")
	}
	err = cache.DeleteCoord(999999)
	if err != nil {
		t.Fatal(err)
	}
	cache.Close()

	cache, err = newDeltaCoordsCache(cache_dir)
	if err != nil {
		t.Fatal()
	}
	defer cache.Close()

	_, err = cache.GetCoord(999999)
	if err != NotFound {
		t.Fatal("deleted node returned not NotFound")
	}

}
