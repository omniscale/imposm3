package pbf

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"compress/zlib"
	"fmt"
	"goposm/parser/pbf/osmpbf"
	"io"
	"log"
	"os"
	"testing"
)

func BenchmarkHello(b *testing.B) {
	b.StopTimer()
	pbf, err := Open("../azores.osm.pbf")
	if err != nil {
		panic(err)
	}

	for pos := range pbf.BlockPositions() {
		fmt.Println(pos.Size, pos.Offset)
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			ReadPrimitiveBlock(pos)
		}
		return
		// for {
		// 	stringtable := NewStringTable(block.GetStringtable())

		// 	for _, group := range block.Primitivegroup {
		// 		dense := group.GetDense()
		// 		ReadDenseNodes(dense, block, stringtable)
		// 	}
		// }
		// return
	}

}

func BenchmarkPrimitiveBlock(b *testing.B) {
	b.StopTimer()

	file, err := os.Open("../azores.osm.pbf")
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	var block = &osmpbf.PrimitiveBlock{}
	var blob = &osmpbf.Blob{}

	var size = 56092
	var offset int64 = 197

	blobData := make([]byte, size)
	file.Seek(offset, 0)
	io.ReadFull(file, blobData)
	err = proto.Unmarshal(blobData, blob)
	if err != nil {
		log.Panic("unmarshaling error blob: ", err)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
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
	}
}
