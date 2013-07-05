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

type idRef struct {
	id     int64
	ref    int64
	delete bool
}

type idRefs struct {
	id   int64
	refs []int64
}

// idRefBunch stores multiple IdRefs
type idRefBunch struct {
	id     int64 // the bunch id
	idRefs []idRefs
}

// idRefBunches can hold multiple idRefBunch
type idRefBunches map[int64]idRefBunch

func (bunches *idRefBunches) add(bunchId, id, ref int64) {
	idRefs := bunches.getIdRefsCreateMissing(bunchId, id)
	idRefs.refs = insertRefs(idRefs.refs, ref)
}

func (bunches *idRefBunches) delete(bunchId, id int64) {
	idRefs := bunches.getIdRefsCreateMissing(bunchId, id)
	idRefs.refs = nil
}

func (bunches *idRefBunches) getIdRefsCreateMissing(bunchId, id int64) *idRefs {
	bunch, ok := (*bunches)[bunchId]
	if !ok {
		bunch = idRefBunch{id: bunchId}
	}
	var result *idRefs

	i := sort.Search(len(bunch.idRefs), func(i int) bool {
		return bunch.idRefs[i].id >= id
	})
	if i < len(bunch.idRefs) && bunch.idRefs[i].id >= id {
		if bunch.idRefs[i].id == id {
			result = &bunch.idRefs[i]
		} else {
			bunch.idRefs = append(bunch.idRefs, idRefs{})
			copy(bunch.idRefs[i+1:], bunch.idRefs[i:])
			bunch.idRefs[i] = idRefs{id: id}
			result = &bunch.idRefs[i]
		}
	} else {
		bunch.idRefs = append(bunch.idRefs, idRefs{id: id})
		result = &bunch.idRefs[len(bunch.idRefs)-1]
	}

	(*bunches)[bunchId] = bunch
	return result
}

var idRefBunchesPool chan idRefBunches

func init() {
	idRefBunchesPool = make(chan idRefBunches, 1)
}

// bunchRefCache
type bunchRefCache struct {
	cache
	buffer    idRefBunches
	write     chan idRefBunches
	add       chan idRef
	mu        sync.Mutex
	waitAdd   *sync.WaitGroup
	waitWrite *sync.WaitGroup
}

func newRefIndex(path string, opts *cacheOptions) (*bunchRefCache, error) {
	index := bunchRefCache{}
	index.options = opts
	err := index.open(path)
	if err != nil {
		return nil, err
	}
	index.write = make(chan idRefBunches, 2)
	index.buffer = make(idRefBunches, bufferSize)
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
	bunchRefCache
}
type WaysRefIndex struct {
	bunchRefCache
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
		index.add <- idRef{id: node.Id, ref: way.Id}
	}
}

func (index *WaysRefIndex) AddFromMembers(relId int64, members []element.Member) {
	for _, member := range members {
		if member.Type == element.WAY {
			index.add <- idRef{id: member.Id, ref: relId}
		}
	}
}

func (index *bunchRefCache) writer() {
	for buffer := range index.write {
		if err := index.writeRefs(buffer); err != nil {
			log.Println("error while writing ref index", err)
		}
	}
	index.waitWrite.Done()
}

func (index *bunchRefCache) Close() {
	close(index.add)
	index.waitAdd.Wait()
	close(index.write)
	index.waitWrite.Wait()
	index.cache.Close()
}

func (index *bunchRefCache) dispatch() {
	for idRef := range index.add {
		if idRef.delete {
			index.buffer.delete(index.getBunchId(idRef.id), idRef.id)
		} else {
			index.buffer.add(index.getBunchId(idRef.id), idRef.id, idRef.ref)
		}
		if len(index.buffer) >= bufferSize {
			index.write <- index.buffer
			select {
			case index.buffer = <-idRefBunchesPool:
			default:
				index.buffer = make(idRefBunches, bufferSize)
			}
		}
	}
	if len(index.buffer) > 0 {
		index.write <- index.buffer
		index.buffer = nil
	}
	index.waitAdd.Done()
}

func (index *bunchRefCache) AddFromWay(way *element.Way) {
	for _, node := range way.Nodes {
		index.add <- idRef{id: node.Id, ref: way.Id}
	}
}

func (index *bunchRefCache) getBunchId(id int64) int64 {
	return id / 64
}

type loadBunchItem struct {
	bunchId int64
	bunch   idRefBunch
}

type writeBunchItem struct {
	bunchIdBuf []byte
	data       []byte
}

func (index *bunchRefCache) writeRefs(idRefs idRefBunches) error {
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
		case idRefBunchesPool <- idRefs:
		}
	}()
	return index.db.Write(index.wo, batch)
}

func mergeBunch(bunch, newBunch []idRefs) []idRefs {
	lastIdx := 0

NextIdRef:
	// for each new idRef...
	for _, newIdRefs := range newBunch {
		// search place in bunch
		for i := lastIdx; i < len(bunch); i++ {
			if bunch[i].id == newIdRefs.id {
				// id already present
				if len(newIdRefs.refs) == 0 {
					// no new refs -> delete
					bunch = append(bunch[:i], bunch[i+1:]...)
				} else { // otherwise add refs
					for _, r := range newIdRefs.refs {
						bunch[i].refs = insertRefs(bunch[i].refs, r)
					}
				}
				lastIdx = i
				continue NextIdRef
			}
			if bunch[i].id > newIdRefs.id {
				// insert before
				if len(newIdRefs.refs) > 0 {
					bunch = append(bunch, idRefs{})
					copy(bunch[i+1:], bunch[i:])
					bunch[i] = newIdRefs
				}
				lastIdx = i
				continue NextIdRef
			}
		}
		// insert at the end
		if len(newIdRefs.refs) > 0 {
			bunch = append(bunch, newIdRefs)
			lastIdx = len(bunch) - 1
		}
	}
	return bunch
}

func (index *bunchRefCache) loadMergeMarshal(keyBuf []byte, newBunch []idRefs) []byte {
	data, err := index.db.Get(index.ro, keyBuf)
	if err != nil {
		panic(err)
	}

	var bunch []idRefs

	if data != nil {
		bunch = unmarshalBunch(data)
	}

	if bunch == nil {
		bunch = newBunch
	} else {
		bunch = mergeBunch(bunch, newBunch)
	}

	data = marshalBunch(bunch)
	return data
}

func (index *bunchRefCache) Get(id int64) []int64 {
	keyBuf := idToKeyBuf(index.getBunchId(id))

	data, err := index.db.Get(index.ro, keyBuf)
	if err != nil {
		panic(err)
	}

	if data != nil {
		for _, idRef := range unmarshalBunch(data) {
			if idRef.id == id {
				return idRef.refs
			}
		}
	}
	return nil
}

func (index *bunchRefCache) Delete(id int64) {
	index.add <- idRef{id: id, delete: true}
}

func marshalBunch(idRefs []idRefs) []byte {
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

func unmarshalBunch(buf []byte) []idRefs {

	r := bytes.NewBuffer(buf)
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil
	}

	idRefs := make([]idRefs, n)

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
