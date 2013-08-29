package binary

import (
	"bytes"
	"encoding/binary"

	"imposm3/element"
)

func MarshalIdRefsBunch(idRefs []element.IdRefs) []byte {
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
		nextPos += binary.PutVarint(buf[nextPos:], idRef.Id-lastId)
		lastId = idRef.Id
	}
	for _, idRef := range idRefs {
		if len(buf)-nextPos < binary.MaxVarintLen64 {
			tmp := make([]byte, len(buf)*2)
			copy(tmp, buf)
			buf = tmp
		}
		nextPos += binary.PutUvarint(buf[nextPos:], uint64(len(idRef.Refs)))
	}
	for _, idRef := range idRefs {
		for _, ref := range idRef.Refs {
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

func UnmarshalIdRefsBunch(buf []byte) []element.IdRefs {
	r := bytes.NewBuffer(buf)
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil
	}

	idRefs := make([]element.IdRefs, n)

	last := int64(0)
	for i := 0; uint64(i) < n; i++ {
		idRefs[i].Id, err = binary.ReadVarint(r)
		if err != nil {
			panic(err)
		}
		idRefs[i].Id += last
		last = idRefs[i].Id
	}
	var numRefs uint64
	for i := 0; uint64(i) < n; i++ {
		numRefs, err = binary.ReadUvarint(r)
		if err != nil {
			panic(err)
		}
		idRefs[i].Refs = make([]int64, numRefs)
	}
	last = 0
	for idIdx := 0; uint64(idIdx) < n; idIdx++ {
		for refIdx := 0; refIdx < len(idRefs[idIdx].Refs); refIdx++ {
			idRefs[idIdx].Refs[refIdx], err = binary.ReadVarint(r)
			if err != nil {
				panic(err)
			}
			idRefs[idIdx].Refs[refIdx] += last
			last = idRefs[idIdx].Refs[refIdx]
		}
	}
	return idRefs
}
