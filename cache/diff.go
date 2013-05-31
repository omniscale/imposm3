package cache

import (
	"bytes"
	"encoding/binary"
	"github.com/jmhodges/levigo"
	"goposm/element"
	"io"
	"log"
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
	Coords *CoordsRefIndex
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
	c.Coords, err = NewCoordsRefIndex(filepath.Join(c.Dir, "coords_index"))
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
	RefIndex
}
type WaysRefIndex struct {
	RefIndex
}

type idRef struct {
	id  int64
	ref int64
}

const cacheSize = 4096

func NewRefIndex(path string) (*RefIndex, error) {
	index := RefIndex{}
	err := index.open(path)
	if err != nil {
		return nil, err
	}
	index.write = make(chan map[int64][]int64, 2)
	index.cache = make(map[int64][]int64, cacheSize)
	index.add = make(chan idRef, 1024)

	index.waitWrite = &sync.WaitGroup{}
	index.waitAdd = &sync.WaitGroup{}
	index.waitWrite.Add(1)
	index.waitAdd.Add(1)

	go index.writer()
	go index.dispatch()
	return &index, nil
}

func NewCoordsRefIndex(dir string) (*CoordsRefIndex, error) {
	cache, err := NewRefIndex(dir)
	refIdx, err := &CoordsRefIndex{*cache}, err
	return refIdx, err
}

func NewWaysRefIndex(dir string) (*WaysRefIndex, error) {
	cache, err := NewRefIndex(dir)
	return &WaysRefIndex{*cache}, err
}

func (index *RefIndex) writer() {
	for cache := range index.write {
		if err := index.writeRefs(cache); err != nil {
			log.Println("error while writing ref index", err)
		}
	}
	index.waitWrite.Done()
}

func (index *RefIndex) Close() {
	close(index.add)
	index.waitAdd.Wait()
	close(index.write)
	index.waitWrite.Wait()
	index.Cache.Close()
}

func (index *RefIndex) dispatch() {
	for idRef := range index.add {
		index.addToCache(idRef.id, idRef.ref)
		if len(index.cache) >= cacheSize {
			index.write <- index.cache
			index.cache = make(map[int64][]int64, cacheSize)
		}
	}
	if len(index.cache) > 0 {
		index.write <- index.cache
		index.cache = nil
	}
	index.waitAdd.Done()
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

func (index *RefIndex) addToCache(id, ref int64) {
	refs, ok := index.cache[id]
	if !ok {
		refs = make([]int64, 0, 1)
	}
	refs = insertRefs(refs, ref)

	index.cache[id] = refs
}

func (index *RefIndex) writeRefs(idRefs map[int64][]int64) error {
	batch := levigo.NewWriteBatch()
	defer batch.Close()

	for id, refs := range idRefs {
		keyBuf := idToKeyBuf(id)
		data := index.loadAppendMarshal(id, refs)
		batch.Put(keyBuf, data)
	}
	return index.db.Write(index.wo, batch)

}
func (index *RefIndex) loadAppendMarshal(id int64, newRefs []int64) []byte {
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
		refs = newRefs
	} else {
		refs = append(refs, newRefs...)
		sort.Sort(Refs(refs))
	}

	data = MarshalRefs(refs)
	return data
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
	if err != nil {
		panic(err)
	}
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
			copy(tmp, buf)
			buf = tmp
		}
		nextPos += binary.PutVarint(buf[nextPos:], ref-lastRef)
		lastRef = ref
	}
	return buf[:nextPos]
}
