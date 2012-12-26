package main

import (
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"fmt"
	// "goposm/osmpbf/fileformat"
	"bytes"
	"compress/zlib"
	"io/ioutil"
	"log"
	"os"
	"osmpbf"
)

func nextBlobHeader(file *os.File, offset int64) (position int64, size int32) {
	newOffset, err := file.Seek(offset, 0)
	if offset != newOffset {
		log.Fatal(err)
		return
	}
	position = offset + 4
	binary.Read(file, binary.BigEndian, &size)
	return
}

func blockPositions(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	var offset int64
	var blobHeader = &osmpbf.BlobHeader{}
	var blob = &osmpbf.Blob{}
	var block = &osmpbf.PrimitiveBlock{}

	var nodesCounter, relationsCounter, waysCounter int

	for {
		position, size := nextBlobHeader(file, offset)
		if size == 0 {
			break
		}
		data := make([]byte, size)

		file.Read(data)
		err = proto.Unmarshal(data, blobHeader)
		if err != nil {
			log.Fatal("unmarshaling error: ", err)
		}

		offset = position + int64(size) + int64(blobHeader.GetDatasize())

		if blobHeader.GetType() == "OSMHeader" {
			fmt.Println("Skip header", blobHeader.GetType())
			continue
		}

		blobData := make([]byte, blobHeader.GetDatasize())
		file.Read(blobData)
		err = proto.Unmarshal(blobData, blob)
		if err != nil {
			log.Fatal("unmarshaling error: ", err)
		}

		buf := bytes.NewBuffer(blob.GetZlibData())
		r, err := zlib.NewReader(buf)
		if err != nil {
			log.Fatal("zlib error: ", err)
		}
		// raw := make([]byte, blob.GetRawSize())
		raw, err := ioutil.ReadAll(r)

		// bytes, err := r.Read(raw)
		if err != nil {
			log.Fatal("zlib read error: ", err)
		}
		err = proto.Unmarshal(raw, block)
		if err != nil {
			log.Fatal("unmarshaling error: ", err)
		}

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
