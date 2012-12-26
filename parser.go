package main

import (
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"fmt"
	// "goposm/osmpbf/fileformat"
	"bytes"
	"compress/zlib"
	"io"
	"log"
	"os"
	"osmpbf"
)

type PBF struct {
	file     *os.File
	filename string
	offset   int64
}

func Open(filename string) (f *PBF, err error) {
	f = new(PBF)
	f.filename = filename
	file, err := os.Open(filename)
	f.file = file
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (pbf *PBF) NextDataPosition() (offset int64, size int32) {
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

func (pbf *PBF) nextBlobHeaderSize() (size int32) {
	pbf.offset += 4
	binary.Read(pbf.file, binary.BigEndian, &size)
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

func ReadPrimitiveBlock(file *os.File, offset int64, size int32) *osmpbf.PrimitiveBlock {
	var block = &osmpbf.PrimitiveBlock{}
	var blob = &osmpbf.Blob{}

	blobData := make([]byte, size)
	file.Seek(offset, 0)
	io.ReadFull(file, blobData)
	err := proto.Unmarshal(blobData, blob)
	if err != nil {
		log.Fatal("unmarshaling error blob: ", err)
	}

	buf := bytes.NewBuffer(blob.GetZlibData())
	r, err := zlib.NewReader(buf)
	if err != nil {
		log.Fatal("zlib error: ", err)
	}
	raw := make([]byte, blob.GetRawSize())
	io.ReadFull(r, raw)

	if err != nil {
		log.Fatal("zlib read error: ", err)
	}
	err = proto.Unmarshal(raw, block)
	if err != nil {
		log.Fatal("unmarshaling error: ", err)
	}

	return block
}

func blockPositions(filename string) {
	pbf, err := Open(filename)

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}

	var nodesCounter, relationsCounter, waysCounter int

	for {
		offset, size := pbf.NextDataPosition()
		if size == 0 {
			break
		}
		block := ReadPrimitiveBlock(file, offset, size)

		for _, group := range block.Primitivegroup {
			dense := group.GetDense()
			if dense != nil {
				nodesCounter += len(dense.Id)
			}
			nodesCounter += len(group.Nodes)
			waysCounter += len(group.Ways)
			relationsCounter += len(group.Relations)
		}
	}
	fmt.Printf("nodes: %v\tways: %v\trelations:%v\n", nodesCounter, waysCounter, relationsCounter)
}

func main() {
	blockPositions(os.Args[1])
	fmt.Println("done")
	// osmpbf
}
