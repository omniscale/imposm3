package cache

import (
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"testing"

	osm "github.com/omniscale/go-osm"
)

func mknode(id int64) osm.Node {
	return osm.Node{
		Element: osm.Element{
			ID: id,
		},
		Long: 8,
		Lat:  10,
	}
}

func TestRemoveSkippedNodes(t *testing.T) {
	nodes := []osm.Node{
		mknode(0),
		mknode(1),
		mknode(-1),
		mknode(2),
		mknode(-1),
	}
	nodes = removeSkippedNodes(nodes)
	if l := len(nodes); l != 3 {
		t.Fatal(nodes)
	}
	if nodes[0].ID != 0 || nodes[1].ID != 1 || nodes[2].ID != 2 {
		t.Fatal(nodes)
	}

	nodes = []osm.Node{
		mknode(-1),
		mknode(-1),
	}
	nodes = removeSkippedNodes(nodes)
	if l := len(nodes); l != 0 {
		t.Fatal(nodes)
	}

	nodes = []osm.Node{
		mknode(-1),
		mknode(1),
		mknode(-1),
		mknode(-1),
		mknode(-1),
		mknode(2),
	}
	nodes = removeSkippedNodes(nodes)
	if l := len(nodes); l != 2 {
		t.Fatal(nodes)
	}
	if nodes[0].ID != 1 || nodes[1].ID != 2 {
		t.Fatal(nodes)
	}
}

func TestReadWriteDeltaCoords(t *testing.T) {
	checkReadWriteDeltaCoords(t, false)
}

func TestReadWriteDeltaCoordsLinearImport(t *testing.T) {
	checkReadWriteDeltaCoords(t, true)
}

func checkReadWriteDeltaCoords(t *testing.T, withLinearImport bool) {
	cacheDir, _ := ioutil.TempDir("", "imposm_test")
	defer os.RemoveAll(cacheDir)

	cache, err := newDeltaCoordsCache(cacheDir)
	if err != nil {
		t.Fatal()
	}

	if withLinearImport {
		cache.SetLinearImport(true)
	}

	// create list with nodes from ID 0->999 in random order
	nodeIDs := rand.Perm(1000)
	nodes := make([]osm.Node, 1000)
	for i := 0; i < len(nodes); i++ {
		nodes[i] = mknode(int64(nodeIDs[i]))
	}

	// add nodes in batches of ten
	for i := 0; i <= len(nodes)-10; i = i + 10 {
		// sort each batch as required by PutCoords
		sort.Sort(byID(nodes[i : i+10]))
		cache.PutCoords(nodes[i : i+10])
	}

	if withLinearImport {
		cache.SetLinearImport(false)
	}

	for i := 0; i < len(nodes); i++ {
		data, err := cache.GetCoord(int64(i))
		if err == NotFound {
			t.Fatal("missing coord:", i)
		} else if err != nil {
			t.Fatal(err)
		}
		if data.ID != int64(i) {
			t.Errorf("unexpected result of GetNode: %v", data)
		}
	}

	// test overwrite
	insertAndCheck(t, cache, 100, 50, 50)

	// test delete
	_, err = cache.GetCoord(999999)
	if err != NotFound {
		t.Error("found missing node")
	}
	insertAndCheck(t, cache, 999999, 10, 10)
	deleteAndCheck(t, cache, 999999)
}

func insertAndCheck(t *testing.T, cache *DeltaCoordsCache, id int64, lon, lat float64) {
	newNode := mknode(id)
	newNode.Long = lon
	newNode.Lat = lat

	err := cache.PutCoords([]osm.Node{newNode})
	if err != nil {
		t.Errorf("error during PutCoords for %v: %s", newNode, err)
	}

	result, err := cache.GetCoord(id)
	if err != nil {
		t.Errorf("got error after getting inserted node %d: %s", id, err)
	}
	if result == nil || result.Long != lon || result.Lat != lat {
		t.Errorf("invalid coords %f, %f != %v", lon, lat, result)
	}
}

func deleteAndCheck(t *testing.T, cache *DeltaCoordsCache, id int64) {
	err := cache.DeleteCoord(id)
	if err != nil {
		t.Errorf("error during DeleteCoord for %d: %s", id, err)
	}

	result, err := cache.GetCoord(id)
	if err != NotFound {
		t.Error("found deleted coord", result)
	}
}

func TestSingleUpdate(t *testing.T) {
	cacheDir, _ := ioutil.TempDir("", "imposm_test")
	defer os.RemoveAll(cacheDir)

	cache, err := newDeltaCoordsCache(cacheDir)
	if err != nil {
		t.Fatal()
	}

	// insert and update in empty batch
	insertAndCheck(t, cache, 123, 10, 10)
	insertAndCheck(t, cache, 123, 10, 11)

	// insert and update in same batch
	insertAndCheck(t, cache, 1, 1, 1)
	insertAndCheck(t, cache, 2, 2, 2)
	insertAndCheck(t, cache, 3, 3, 3)
	insertAndCheck(t, cache, 4, 4, 4)
	insertAndCheck(t, cache, 3, 10, 11)
	insertAndCheck(t, cache, 2, 10, 11)
	insertAndCheck(t, cache, 1, 10, 11)
	insertAndCheck(t, cache, 4, 10, 11)
	// repeat after flushing
	cache.Flush()
	insertAndCheck(t, cache, 1, 1, 1)
	insertAndCheck(t, cache, 2, 2, 2)
	insertAndCheck(t, cache, 3, 3, 3)
	insertAndCheck(t, cache, 4, 4, 4)

}

func BenchmarkWriteDeltaCoords(b *testing.B) {
	b.StopTimer()
	cacheDir, _ := ioutil.TempDir("", "imposm_test")
	defer os.RemoveAll(cacheDir)

	cache, err := newDeltaCoordsCache(cacheDir)
	if err != nil {
		b.Fatal()
	}
	defer cache.Close()

	nodes := make([]osm.Node, 10000)
	for i := range nodes {
		nodes[i].ID = rand.Int63n(50000)
		nodes[i].Long = rand.Float64() - 0.5*360
		nodes[i].Lat = rand.Float64() - 0.5*180
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for _, n := range nodes {
			if err := cache.PutCoords([]osm.Node{n}); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkReadDeltaCoords(b *testing.B) {
	b.StopTimer()
	cacheDir, _ := ioutil.TempDir("", "imposm_test")
	defer os.RemoveAll(cacheDir)

	cache, err := newDeltaCoordsCache(cacheDir)
	if err != nil {
		b.Fatal()
	}
	defer cache.Close()

	nodes := make([]osm.Node, 10000)
	for i := range nodes {
		nodes[i].ID = rand.Int63n(50000)
		nodes[i].Long = rand.Float64() - 0.5*360
		nodes[i].Lat = rand.Float64() - 0.5*180
	}
	for _, n := range nodes {
		if err := cache.PutCoords([]osm.Node{n}); err != nil {
			b.Fatal(err)
		}
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for n := 0; n < 10000; n++ {
			if _, err := cache.GetCoord(int64(n)); err != nil && err != NotFound {
				b.Fatal(err)
			}
		}
	}
}
