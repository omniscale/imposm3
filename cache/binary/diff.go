package binary

import (
	"encoding/binary"

	"github.com/omniscale/imposm3/element"
)

func MarshalIDRefsBunch(idRefs []element.IDRefs) []byte {
	buf := make([]byte, len(idRefs)*(4+1+6)+binary.MaxVarintLen64)

	lastRef := int64(0)
	lastID := int64(0)
	nextPos := 0

	nextPos += binary.PutUvarint(buf[nextPos:], uint64(len(idRefs)))

	for _, idRef := range idRefs {
		if len(buf)-nextPos < binary.MaxVarintLen64 {
			tmp := make([]byte, len(buf)*2)
			copy(tmp, buf)
			buf = tmp
		}
		nextPos += binary.PutVarint(buf[nextPos:], idRef.ID-lastID)
		lastID = idRef.ID
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

func MarshalIDRefsBunch2(idRefs []element.IDRefs, buf []byte) []byte {
	lastRef := int64(0)
	lastID := int64(0)
	nextPos := 0

	estSize := len(idRefs)*(4+1+6) + binary.MaxVarintLen64
	if cap(buf) < estSize {
		buf = make([]byte, estSize)
	} else {
		// expand to full capacity
		buf = buf[:cap(buf)]
	}

	nextPos += binary.PutUvarint(buf[nextPos:], uint64(len(idRefs)))

	for _, idRef := range idRefs {
		if len(buf)-nextPos < binary.MaxVarintLen64 {
			tmp := make([]byte, cap(buf)*2)
			copy(tmp, buf[:nextPos])
			buf = tmp
		}
		nextPos += binary.PutVarint(buf[nextPos:], idRef.ID-lastID)
		lastID = idRef.ID
	}
	for _, idRef := range idRefs {
		if len(buf)-nextPos < binary.MaxVarintLen64 {
			tmp := make([]byte, cap(buf)*2)
			copy(tmp, buf[:nextPos])
			buf = tmp
		}
		nextPos += binary.PutUvarint(buf[nextPos:], uint64(len(idRef.Refs)))
	}
	for _, idRef := range idRefs {
		for _, ref := range idRef.Refs {
			if len(buf)-nextPos < binary.MaxVarintLen64 {
				tmp := make([]byte, cap(buf)*2)
				copy(tmp, buf[:nextPos])
				buf = tmp
			}
			nextPos += binary.PutVarint(buf[nextPos:], ref-lastRef)
			lastRef = ref
		}
	}
	return buf[:nextPos]
}

func UnmarshalIDRefsBunch(buf []byte) []element.IDRefs {
	length, n := binary.Uvarint(buf)
	if n <= 0 {
		return nil
	}

	offset := n

	idRefs := make([]element.IDRefs, length)

	last := int64(0)
	for i := 0; uint64(i) < length; i++ {
		idRefs[i].ID, n = binary.Varint(buf[offset:])
		if n <= 0 {
			panic("no data")
		}
		offset += n
		idRefs[i].ID += last
		last = idRefs[i].ID
	}
	var numRefs uint64
	for i := 0; uint64(i) < length; i++ {
		numRefs, n = binary.Uvarint(buf[offset:])
		if n <= 0 {
			panic("no data")
		}
		offset += n
		idRefs[i].Refs = make([]int64, numRefs)
	}
	last = 0
	for idIdx := 0; uint64(idIdx) < length; idIdx++ {
		for refIdx := 0; refIdx < len(idRefs[idIdx].Refs); refIdx++ {
			idRefs[idIdx].Refs[refIdx], n = binary.Varint(buf[offset:])
			if n <= 0 {
				panic("no data")
			}
			offset += n
			idRefs[idIdx].Refs[refIdx] += last
			last = idRefs[idIdx].Refs[refIdx]
		}
	}
	return idRefs
}

func UnmarshalIDRefsBunch2(buf []byte, idRefs []element.IDRefs) []element.IDRefs {
	length, n := binary.Uvarint(buf)
	if n <= 0 {
		return nil
	}

	offset := n

	if uint64(cap(idRefs)) < length {
		idRefs = make([]element.IDRefs, length)
	} else {
		idRefs = idRefs[:length]
	}

	last := int64(0)
	for i := 0; uint64(i) < length; i++ {
		idRefs[i].ID, n = binary.Varint(buf[offset:])
		if n <= 0 {
			panic("no data")
		}
		offset += n
		idRefs[i].ID += last
		last = idRefs[i].ID
	}
	var numRefs uint64
	for i := 0; uint64(i) < length; i++ {
		numRefs, n = binary.Uvarint(buf[offset:])
		if n <= 0 {
			panic("no data")
		}
		offset += n
		if uint64(cap(idRefs[i].Refs)) < numRefs {
			idRefs[i].Refs = make([]int64, numRefs)
		} else {
			idRefs[i].Refs = idRefs[i].Refs[:numRefs]
		}
	}
	last = 0
	for idIdx := 0; uint64(idIdx) < length; idIdx++ {
		for refIdx := 0; refIdx < len(idRefs[idIdx].Refs); refIdx++ {
			idRefs[idIdx].Refs[refIdx], n = binary.Varint(buf[offset:])
			if n <= 0 {
				panic("no data")
			}
			offset += n
			idRefs[idIdx].Refs[refIdx] += last
			last = idRefs[idIdx].Refs[refIdx]
		}
	}
	return idRefs
}
