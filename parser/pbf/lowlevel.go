package pbf

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"compress/zlib"
	structs "encoding/binary"
	"goposm/parser/pbf/osmpbf"
	"io"
	"log"
	"os"
)

func ReadPrimitiveBlock(pos BlockPosition) *osmpbf.PrimitiveBlock {
	file, err := os.Open(pos.Filename)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	var block = &osmpbf.PrimitiveBlock{}
	var blob = &osmpbf.Blob{}

	blobData := make([]byte, pos.Size)
	file.Seek(pos.Offset, 0)
	io.ReadFull(file, blobData)
	err = proto.Unmarshal(blobData, blob)
	if err != nil {
		log.Panic("unmarshaling error blob: ", err)
	}

	buf := bytes.NewBuffer(blob.GetZlibData())
	r, err := zlib.NewReader(buf)
	if err != nil {
		log.Panic("zlib error: ", err)
	}
	raw := make([]byte, blob.GetRawSize())
	io.ReadFull(r, raw)

	err = proto.Unmarshal(raw, block)
	if err != nil {
		log.Panic("unmarshaling error: ", err)
	}

	return block
}

func (pbf *PBF) BlockPositions() (positions chan BlockPosition) {
	positions = make(chan BlockPosition, 8)
	go func() {
		for {
			offset, size := pbf.NextDataPosition()
			if size == 0 {
				close(positions)
				pbf.Close()
				return
			}
			positions <- BlockPosition{pbf.filename, offset, size}
		}
	}()
	return
}

func (pbf *PBF) nextBlobHeaderSize() (size int32) {
	pbf.offset += 4
	structs.Read(pbf.file, structs.BigEndian, &size)
	return
}

func (pbf *PBF) nextBlobHeader() *osmpbf.BlobHeader {
	var blobHeader = &osmpbf.BlobHeader{}

	size := pbf.nextBlobHeaderSize()
	if size == 0 {
		return blobHeader
	}

	data := make([]byte, size)
	io.ReadFull(pbf.file, data)

	err := proto.Unmarshal(data, blobHeader)
	if err != nil {
		log.Fatal("unmarshaling error (header): ", err)
	}

	pbf.offset += int64(size)
	return blobHeader
}
