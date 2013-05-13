package cache

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
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
	keyBuf := idToKeyBuf(id)
	data, err := index.db.Get(index.ro, keyBuf)
	if err != nil {
		panic(err)
	}

	var refs []int64

	if data != nil {
		refs = UnmarshalRefs(data)
	}

	if refs == nil {
		refs = make([]int64, 0, 1)
	}
	refs = insertRefs(refs, ref)

	data = MarshalRefs(refs)

	err = index.db.Put(index.wo, keyBuf, data)
	return err
}

func insertRefs(refs []int64, ref int64) []int64 {
	i := sort.Search(len(refs), func(i int) bool {
		return refs[i] >= ref
	})
	if i < len(refs) && refs[i] >= ref {
		refs = append(refs, 0)
		copy(refs[i+1:], refs[i:])
		refs[i] = ref
	} else {
		refs = append(refs, ref)
	}
	return refs
}

func (index *RefIndex) Get(id int64) []int64 {
	keyBuf := idToKeyBuf(id)
	data, err := index.db.Get(index.ro, keyBuf)
	var refs []int64
	if data != nil {
		refs = UnmarshalRefs(data)
		if err != nil {
			panic(err)
		}
	}
	return refs
}

func UnmarshalRefs(buf []byte) []int64 {
	refs := make([]int64, 0, 8)

	r := bytes.NewBuffer(buf)

	lastRef := int64(0)
	for {
		ref, err := binary.ReadVarint(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println("error while unmarshaling refs:", err)
			break
		}
		ref = lastRef + ref
		refs = append(refs, ref)
		lastRef = ref
	}

	return refs
}

func MarshalRefs(refs []int64) []byte {
	buf := make([]byte, len(refs)*4+binary.MaxVarintLen64)

	lastRef := int64(0)
	nextPos := 0
	for _, ref := range refs {
		if len(buf)-nextPos < binary.MaxVarintLen64 {
			tmp := make([]byte, len(buf)*2)
			buf = append(tmp, buf...)
		}
		nextPos += binary.PutVarint(buf[nextPos:], ref-lastRef)
		lastRef = ref
	}
	return buf[:nextPos]
}
