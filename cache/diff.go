package cache

import (
	"code.google.com/p/goprotobuf/proto"
	bin "encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type DiffCache struct {
	Dir    string
	Coords *RefIndex
	Ways   *RefIndex
	opened bool
}

func (c *DiffCache) Close() {
	if c.Coords != nil {
		c.Coords.close()
		c.Coords = nil
	}
	if c.Ways != nil {
		c.Ways.close()
		c.Ways = nil
	}
}

func NewDiffCache(dir string) *DiffCache {
	cache := &DiffCache{Dir: dir}
	return cache
}

func (c *DiffCache) Open() error {
	var err error
	c.Coords, err = NewRefIndex(filepath.Join(c.Dir, "coords_index"))
	if err != nil {
		c.Close()
		return err
	}
	c.Ways, err = NewRefIndex(filepath.Join(c.Dir, "ways_index"))
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
	mu sync.Mutex
}

func NewRefIndex(path string) (*RefIndex, error) {
	index := RefIndex{}
	err := index.open(path)
	if err != nil {
		return nil, err
	}
	return &index, nil
}

func (index *RefIndex) Add(id, ref int64) error {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, id)
	data, err := index.db.Get(index.ro, keyBuf)
	if err != nil {
		panic(err)
	}
	refs := &Refs{}
	if data != nil {
		err = proto.Unmarshal(data, refs)
		if err != nil {
			panic(err)
		}
	}

	if refs.Ids == nil {
		refs.Ids = make([]int64, 0, 1)
	}
	// TODO change to delta encoding
	refs.insertId(ref)

	data, err = proto.Marshal(refs)
	if err != nil {
		panic(err)
	}
	err = index.db.Put(index.wo, keyBuf, data)
	return err
}

func (r *Refs) insertId(ref int64) {
	i := sort.Search(len(r.Ids), func(i int) bool {
		return r.Ids[i] >= ref
	})
	if i < len(r.Ids) && r.Ids[i] >= ref {
		r.Ids = append(r.Ids, 0)
		copy(r.Ids[i+1:], r.Ids[i:])
		r.Ids[i] = ref
	} else {
		r.Ids = append(r.Ids, ref)
	}
}

func (index *RefIndex) Get(id int64) []int64 {
	keyBuf := make([]byte, 8)
	bin.PutVarint(keyBuf, id)
	data, err := index.db.Get(index.ro, keyBuf)
	refs := &Refs{}
	if data != nil {
		err = proto.Unmarshal(data, refs)
		if err != nil {
			panic(err)
		}
	}
	return refs.Ids
}
