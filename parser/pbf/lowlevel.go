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

func readPrimitiveBlock(pos Block) *osmpbf.PrimitiveBlock {
	file, err := os.Open(pos.filename)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	var block = &osmpbf.PrimitiveBlock{}
	var blob = &osmpbf.Blob{}

	blobData := make([]byte, pos.size)
	file.Seek(pos.offset, 0)
	io.ReadFull(file, blobData)
	err = proto.Unmarshal(blobData, blob)
	if err != nil {
		log.Panic("unmarshaling error blob: ", err)
	}

	// pbf contains (uncompressed) raw or zlibdata
	raw := blob.GetRaw()
	if raw == nil {
		buf := bytes.NewBuffer(blob.GetZlibData())
		r, err := zlib.NewReader(buf)
		if err != nil {
			log.Panic("zlib error: ", err)
		}
		raw = make([]byte, blob.GetRawSize())
		_, err = io.ReadFull(r, raw)
		if err != nil {
			log.Panic("zlib read error: ", err)
		}
	}

	err = proto.Unmarshal(raw, block)
	if err != nil {
		log.Panic("unmarshaling error: ", err)
	}

	return block
}

type pbf struct {
	file     *os.File
	filename string
	offset   int64
}

func open(filename string) (f *pbf, err error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	f = &pbf{filename: filename, file: file}
	return f, nil
}

func (pbf *pbf) Close() error {
	return pbf.file.Close()
}

func (pbf *pbf) NextDataPosition() (offset int64, size int32) {
	header := pbf.nextBlobHeader()
	size = header.GetDatasize()
	offset = pbf.offset

	pbf.offset += int64(size)
	pbf.file.Seek(pbf.offset, 0)

	if header.GetType() == "OSMHeader" {
		return pbf.NextDataPosition()
	}
	return
}

func (pbf *pbf) BlockPositions() (positions chan Block) {
	positions = make(chan Block, 8)
	go func() {
		for {
			offset, size := pbf.NextDataPosition()
			if size == 0 {
				close(positions)
				pbf.Close()
				return
			}
			positions <- Block{pbf.filename, offset, size}
		}
	}()
	return
}

func (pbf *pbf) nextBlobHeaderSize() (size int32) {
	pbf.offset += 4
	structs.Read(pbf.file, structs.BigEndian, &size)
	return
}

func (pbf *pbf) nextBlobHeader() *osmpbf.BlobHeader {
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
