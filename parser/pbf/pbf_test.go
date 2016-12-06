package pbf

import (
	"bytes"
	"compress/zlib"
	"io"
	"log"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/omniscale/imposm3/parser/pbf/internal/osmpbf"
)

func BenchmarkHello(b *testing.B) {
	b.StopTimer()
	pbf, err := open("./monaco-20150428.osm.pbf")
	if err != nil {
		panic(err)
	}

	for pos := range pbf.BlockPositions() {
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			readPrimitiveBlock(pos)
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

	file, err := os.Open("./monaco-20150428.osm.pbf")
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	var block = &osmpbf.PrimitiveBlock{}
	var blob = &osmpbf.Blob{}

	var size = 79566
	var offset int64 = 155

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
