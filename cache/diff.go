package cache

import (
	"goposm/element"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type Refs []int64

func (a Refs) Len() int           { return len(a) }
func (a Refs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Refs) Less(i, j int) bool { return a[i] < a[j] }

type DiffCache struct {
	Dir    string
	Coords *BunchRefCache
	Ways   *WaysRefIndex
	opened bool
}

func (c *DiffCache) Close() {
	if c.Coords != nil {
		c.Coords.Close()
		c.Coords = nil
	}
	if c.Ways != nil {
		c.Ways.Close()
		c.Ways = nil
	}
}

func NewDiffCache(dir string) *DiffCache {
	cache := &DiffCache{Dir: dir}
	return cache
}

func (c *DiffCache) Open() error {
	var err error
	c.Coords, err = NewBunchRefCache(filepath.Join(c.Dir, "coords_index"), &osmCacheOptions.CoordsIndex)
	if err != nil {
		c.Close()
		return err
	}
	c.Ways, err = NewWaysRefIndex(filepath.Join(c.Dir, "ways_index"))
	if err != nil {
		c.Close()
		return err
	}
	c.opened = true
	return nil
}

func (c *DiffCache) Exists() bool {
	if c.opened {
		return true
	}
	if _, err := os.Stat(filepath.Join(c.Dir, "coords_index")); !os.IsNotExist(err) {
		return true
	}
	if _, err := os.Stat(filepath.Join(c.Dir, "ways_index")); !os.IsNotExist(err) {
		return true
	}
	return false
}

func (c *DiffCache) Remove() error {
	if c.opened {
		c.Close()
	}
	if err := os.RemoveAll(filepath.Join(c.Dir, "coords_index")); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(c.Dir, "ways_index")); err != nil {
		return err
	}
	return nil
}

type RefIndex struct {
	Cache
	cache     map[int64][]int64
	write     chan map[int64][]int64
	add       chan idRef
	mu        sync.Mutex
	waitAdd   *sync.WaitGroup
	waitWrite *sync.WaitGroup
}

type CoordsRefIndex struct {
	BunchRefCache
}
type WaysRefIndex struct {
	BunchRefCache
}

type idRef struct {
	id  int64
	ref int64
}

const cacheSize = 1024

var refCaches chan map[int64][]int64

func init() {
	refCaches = make(chan map[int64][]int64, 1)
}

func NewCoordsRefIndex(dir string) (*CoordsRefIndex, error) {
	cache, err := NewBunchRefCache(dir, &osmCacheOptions.CoordsIndex)
	if err != nil {
		return nil, err
	}
	return &CoordsRefIndex{*cache}, nil
}

func NewWaysRefIndex(dir string) (*WaysRefIndex, error) {
	cache, err := NewBunchRefCache(dir, &osmCacheOptions.WaysIndex)
	if err != nil {
		return nil, err
	}
	return &WaysRefIndex{*cache}, nil
}

func (index *CoordsRefIndex) AddFromWay(way *element.Way) {
	for _, node := range way.Nodes {
		index.add <- idRef{node.Id, way.Id}
	}
}

func (index *WaysRefIndex) AddFromMembers(relId int64, members []element.Member) {
	for _, member := range members {
		if member.Type == element.WAY {
			index.add <- idRef{member.Id, relId}
		}
	}
}

func insertRefs(refs []int64, ref int64) []int64 {
	i := sort.Search(len(refs), func(i int) bool {
		return refs[i] >= ref
	})
	if i < len(refs) && refs[i] >= ref {
		if refs[i] == ref {
			return refs
		}
		refs = append(refs, 0)
		copy(refs[i+1:], refs[i:])
		refs[i] = ref
	} else {
		refs = append(refs, ref)
	}
	return refs
}
