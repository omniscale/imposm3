package cache

import (
	"code.google.com/p/goprotobuf/proto"
	bin "encoding/binary"
	"sort"
	"sync"
)

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
