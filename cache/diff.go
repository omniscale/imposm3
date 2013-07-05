package cache

import (
	"bytes"
	"encoding/binary"
	"github.com/jmhodges/levigo"
	"goposm/element"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
)

type byInt64 []int64

func (a byInt64) Len() int           { return len(a) }
func (a byInt64) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byInt64) Less(i, j int) bool { return a[i] < a[j] }

type DiffCache struct {
	Dir    string
	Coords *CoordsRefIndex
	Ways   *WaysRefIndex
	opened bool
}

func NewDiffCache(dir string) *DiffCache {
	cache := &DiffCache{Dir: dir}
	return cache
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

func (c *DiffCache) Open() error {
	var err error
	c.Coords, err = newCoordsRefIndex(filepath.Join(c.Dir, "coords_index"))
	if err != nil {
		c.Close()
		return err
	}
	c.Ways, err = newWaysRefIndex(filepath.Join(c.Dir, "ways_index"))
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

type idRef struct {
	id  int64
	ref int64
}

const bufferSize = 64 * 1024

func insertRefs(refs []int64, ref int64) []int64 {
	i := sort.Search(len(refs), func(i int) bool {
		return refs[i] >= ref
	})
	if i < len(refs) && refs[i] >= ref {
		if refs[i] > ref {
			refs = append(refs, 0)
			copy(refs[i+1:], refs[i:])
			refs[i] = ref
		} // else already inserted
	} else {
		refs = append(refs, ref)
	}
	return refs
}

type IdRef struct {
	id   int64
	refs []int64
}

// IdRefBunch stores multiple IdRefs
type IdRefBunch struct {
	id     int64 // the bunch id
	idRefs []IdRef
}

// IdRefBunches can hold multiple IdRefBunch
type IdRefBunches map[int64]IdRefBunch

func (bunches *IdRefBunches) add(bunchId, id, ref int64) {
	bunch, ok := (*bunches)[bunchId]
	if !ok {
		bunch = IdRefBunch{id: bunchId}
	}
	var idRef *IdRef

	i := sort.Search(len(bunch.idRefs), func(i int) bool {
		return bunch.idRefs[i].id >= id
	})
	if i < len(bunch.idRefs) && bunch.idRefs[i].id >= id {
		if bunch.idRefs[i].id == id {
			idRef = &bunch.idRefs[i]
		} else {
			bunch.idRefs = append(bunch.idRefs, IdRef{})
			copy(bunch.idRefs[i+1:], bunch.idRefs[i:])
			bunch.idRefs[i] = IdRef{id: id}
			idRef = &bunch.idRefs[i]
		}
	} else {
		bunch.idRefs = append(bunch.idRefs, IdRef{id: id})
		idRef = &bunch.idRefs[len(bunch.idRefs)-1]
	}

	idRef.refs = insertRefs(idRef.refs, ref)
	(*bunches)[bunchId] = bunch
}

var IdRefBunchesPool chan IdRefBunches

func init() {
	IdRefBunchesPool = make(chan IdRefBunches, 1)
}

// BunchRefCache
type BunchRefCache struct {
	cache
	buffer    IdRefBunches
	write     chan IdRefBunches
	add       chan idRef
	mu        sync.Mutex
	waitAdd   *sync.WaitGroup
	waitWrite *sync.WaitGroup
}

func newRefIndex(path string, opts *cacheOptions) (*BunchRefCache, error) {
	index := BunchRefCache{}
	index.options = opts
	err := index.open(path)
	if err != nil {
		return nil, err
	}
	index.write = make(chan IdRefBunches, 2)
	index.buffer = make(IdRefBunches, bufferSize)
	index.add = make(chan idRef, 1024)

	index.waitWrite = &sync.WaitGroup{}
	index.waitAdd = &sync.WaitGroup{}
	index.waitWrite.Add(1)
	index.waitAdd.Add(1)

	go index.writer()
	go index.dispatch()
	return &index, nil
}

type CoordsRefIndex struct {
	BunchRefCache
}
type WaysRefIndex struct {
	BunchRefCache
}

func newCoordsRefIndex(dir string) (*CoordsRefIndex, error) {
	cache, err := newRefIndex(dir, &globalCacheOptions.CoordsIndex)
	if err != nil {
		return nil, err
	}
	return &CoordsRefIndex{*cache}, nil
}

func newWaysRefIndex(dir string) (*WaysRefIndex, error) {
	cache, err := newRefIndex(dir, &globalCacheOptions.WaysIndex)
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

func (index *BunchRefCache) writer() {
	for buffer := range index.write {
		if err := index.writeRefs(buffer); err != nil {
			log.Println("error while writing ref index", err)
		}
	}
	index.waitWrite.Done()
}

func (index *BunchRefCache) Close() {
	close(index.add)
	index.waitAdd.Wait()
	close(index.write)
	index.waitWrite.Wait()
	index.cache.Close()
}

func (index *BunchRefCache) dispatch() {
	for idRef := range index.add {
		index.buffer.add(index.getBunchId(idRef.id), idRef.id, idRef.ref)
		if len(index.buffer) >= bufferSize {
			index.write <- index.buffer
			select {
			case index.buffer = <-IdRefBunchesPool:
			default:
				index.buffer = make(IdRefBunches, bufferSize)
			}
		}
	}
	if len(index.buffer) > 0 {
		index.write <- index.buffer
		index.buffer = nil
	}
	index.waitAdd.Done()
}

func (index *BunchRefCache) AddFromWay(way *element.Way) {
	for _, node := range way.Nodes {
		index.add <- idRef{node.Id, way.Id}
	}
}

func (index *BunchRefCache) getBunchId(id int64) int64 {
	return id / 64
}

type loadBunchItem struct {
	bunchId int64
	bunch   IdRefBunch
}

type writeBunchItem struct {
	bunchIdBuf []byte
	data       []byte
}

func (index *BunchRefCache) writeRefs(idRefs IdRefBunches) error {
	batch := levigo.NewWriteBatch()
	defer batch.Close()

	wg := sync.WaitGroup{}
	putc := make(chan writeBunchItem)
	loadc := make(chan loadBunchItem)

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			for item := range loadc {
				keyBuf := idToKeyBuf(item.bunchId)
				putc <- writeBunchItem{
					keyBuf,
					index.loadMergeMarshal(keyBuf, item.bunch.idRefs),
				}
			}
			wg.Done()
		}()
	}

	go func() {
		for bunchId, bunch := range idRefs {
			loadc <- loadBunchItem{bunchId, bunch}
		}
		close(loadc)
		wg.Wait()
		close(putc)
	}()

	for item := range putc {
		batch.Put(item.bunchIdBuf, item.data)
	}

	go func() {
		for k, _ := range idRefs {
			delete(idRefs, k)
		}
		select {
		case IdRefBunchesPool <- idRefs:
		}
	}()
	return index.db.Write(index.wo, batch)
}

func mergeBunch(bunch, newBunch []IdRef) []IdRef {
	lastIdx := 0

NextIdRef:
	// for each new IdRef...
	for _, newIdRefs := range newBunch {
		// search place in bunch
		for i := lastIdx; i < len(bunch); i++ {
			if bunch[i].id == newIdRefs.id {
				// id already present, add refs
				for _, r := range newIdRefs.refs {
					bunch[i].refs = insertRefs(bunch[i].refs, r)
				}
				lastIdx = i
				break NextIdRef
			}
			if bunch[i].id > newIdRefs.id {
				// insert before
				bunch = append(bunch, IdRef{})
				copy(bunch[i+1:], bunch[i:])
				bunch[i] = newIdRefs
				lastIdx = i
				break NextIdRef
			}
		}
		// insert at the end
		bunch = append(bunch, newIdRefs)
		lastIdx = len(bunch) - 1
	}
	return bunch
}

func (index *BunchRefCache) loadMergeMarshal(keyBuf []byte, newBunch []IdRef) []byte {
	data, err := index.db.Get(index.ro, keyBuf)
	if err != nil {
		panic(err)
	}

	var bunch []IdRef

	if data != nil {
		bunch = UnmarshalBunch(data)
	}

	if bunch == nil {
		bunch = newBunch
	} else {
		bunch = mergeBunch(bunch, newBunch)
	}

	data = MarshalBunch(bunch)
	return data
}

func (index *BunchRefCache) Get(id int64) []int64 {
	keyBuf := idToKeyBuf(index.getBunchId(id))

	data, err := index.db.Get(index.ro, keyBuf)
	if err != nil {
		panic(err)
	}

	if data != nil {
		for _, idRef := range UnmarshalBunch(data) {
			if idRef.id == id {
				return idRef.refs
			}
		}
	}
	return nil
}

func MarshalBunch(idRefs []IdRef) []byte {
	buf := make([]byte, len(idRefs)*(4+1+6)+binary.MaxVarintLen64)

	lastRef := int64(0)
	lastId := int64(0)
	nextPos := 0

	nextPos += binary.PutUvarint(buf[nextPos:], uint64(len(idRefs)))

	for _, idRef := range idRefs {
		if len(buf)-nextPos < binary.MaxVarintLen64 {
			tmp := make([]byte, len(buf)*2)
			copy(tmp, buf)
			buf = tmp
		}
		nextPos += binary.PutVarint(buf[nextPos:], idRef.id-lastId)
		lastId = idRef.id
	}
	for _, idRef := range idRefs {
		if len(buf)-nextPos < binary.MaxVarintLen64 {
			tmp := make([]byte, len(buf)*2)
			copy(tmp, buf)
			buf = tmp
		}
		nextPos += binary.PutUvarint(buf[nextPos:], uint64(len(idRef.refs)))
	}
	for _, idRef := range idRefs {
		for _, ref := range idRef.refs {
			if len(buf)-nextPos < binary.MaxVarintLen64 {
				tmp := make([]byte, len(buf)*2)
				copy(tmp, buf)
				buf = tmp
			}
			nextPos += binary.PutVarint(buf[nextPos:], ref-lastRef)
			lastRef = ref
		}
	}
	return buf[:nextPos]
}

func UnmarshalBunch(buf []byte) []IdRef {

	r := bytes.NewBuffer(buf)
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil
	}

	idRefs := make([]IdRef, n)

	last := int64(0)
	for i := 0; uint64(i) < n; i++ {
		idRefs[i].id, err = binary.ReadVarint(r)
		if err != nil {
			panic(err)
		}
		idRefs[i].id += last
		last = idRefs[i].id
	}
	var numRefs uint64
	for i := 0; uint64(i) < n; i++ {
		numRefs, err = binary.ReadUvarint(r)
		if err != nil {
			panic(err)
		}
		idRefs[i].refs = make([]int64, numRefs)
	}
	last = 0
	for idIdx := 0; uint64(idIdx) < n; idIdx++ {
		for refIdx := 0; refIdx < len(idRefs[idIdx].refs); refIdx++ {
			idRefs[idIdx].refs[refIdx], err = binary.ReadVarint(r)
			if err != nil {
				panic(err)
			}
			idRefs[idIdx].refs[refIdx] += last
			last = idRefs[idIdx].refs[refIdx]
		}
	}
	return idRefs
}
