package cache

import (
	"code.google.com/p/goprotobuf/proto"
	bin "encoding/binary"
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
	// TODO check for duplicates
	refs.Ids = append(refs.Ids, ref)

	data, err = proto.Marshal(refs)
	if err != nil {
		panic(err)
	}
	err = index.db.Put(index.wo, keyBuf, data)
	return err
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
