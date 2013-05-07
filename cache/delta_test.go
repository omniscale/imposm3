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

	cache, err := NewDeltaCoordsCache(cache_dir)
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
		sort.Sort(Nodes(nodes[i : i+10]))
		cache.PutCoords(nodes[i : i+10])
	}

	cache.Close()

	cache, err = NewDeltaCoordsCache(cache_dir)
	if err != nil {
		t.Fatal()
	}
	defer cache.Close()

	for i := 0; i < len(nodes); i++ {
		data, ok := cache.GetCoord(int64(i))
		if !ok {
			t.Fatal("missing coord:", i)
		}
		if data.Id != int64(i) {
			t.Errorf("unexpected result of GetNode: %v", data)
		}
	}

	_, ok := cache.GetCoord(999999)
	if ok {
		t.Error("missing node not nil")
	}

}
