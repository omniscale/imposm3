package pbf

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"compress/zlib"
	structs "encoding/binary"
	"errors"
	"fmt"
	"github.com/omniscale/imposm3/parser/pbf/osmpbf"
	"io"
	"log"
	"os"
	"time"
)

type parserError struct {
	message       string
	originalError error
}

func (e *parserError) Error() string {
	return fmt.Sprintf("%s: %v", e.message, e.originalError)
}

func newParserError(message string, err error) *parserError {
	return &parserError{message, err}
}

var supportedFeatured = map[string]bool{"OsmSchema-V0.6": true, "DenseNodes": true}

func readBlobData(pos Block) ([]byte, error) {
	file, err := os.Open(pos.filename)
	if err != nil {
		return nil, newParserError("file open", err)
	}
	defer file.Close()

	var blob = &osmpbf.Blob{}

	blobData := make([]byte, pos.size)
	file.Seek(pos.offset, 0)
	io.ReadFull(file, blobData)
	err = proto.Unmarshal(blobData, blob)
	if err != nil {
		return nil, newParserError("unmarshaling blob", err)
	}

	// pbf contains (uncompressed) raw or zlibdata
	raw := blob.GetRaw()
	if raw == nil {
		buf := bytes.NewBuffer(blob.GetZlibData())
		r, err := zlib.NewReader(buf)
		if err != nil {
			return nil, newParserError("zlib error", err)
		}
		raw = make([]byte, blob.GetRawSize())
		_, err = io.ReadFull(r, raw)
		if err != nil {
			return nil, newParserError("zlib read error", err)
		}
	}
	return raw, nil
}

func readPrimitiveBlock(pos Block) *osmpbf.PrimitiveBlock {
	raw, err := readBlobData(pos)
	if err != nil {
		log.Panic(err)
	}
	block := &osmpbf.PrimitiveBlock{}
	err = proto.Unmarshal(raw, block)
	if err != nil {
		log.Panic("unmarshaling error: ", err)
	}

	return block
}

func readAndParseHeaderBlock(pos Block) (*pbfHeader, error) {
	raw, err := readBlobData(pos)
	if err != nil {
		return nil, err
	}

	header := &osmpbf.HeaderBlock{}
	err = proto.Unmarshal(raw, header)
	if err != nil {
		return nil, err
	}

	for _, feature := range header.RequiredFeatures {
		if supportedFeatured[feature] != true {
			return nil, errors.New("cannot parse file, feature " + feature + " not supported")
		}
	}

	result := &pbfHeader{}
	timestamp := header.GetOsmosisReplicationTimestamp()
	result.Time = time.Unix(timestamp, 0 /* nanoseconds */)
	result.Sequence = header.GetOsmosisReplicationSequenceNumber()
	return result, nil
}

type Pbf struct {
	file     *os.File
	Filename string
	offset   int64
	Header   *pbfHeader
}

type pbfHeader struct {
	Time     time.Time
	Sequence int64
}

func Open(filename string) (f *Pbf, err error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	f = &Pbf{Filename: filename, file: file}
	err = f.parseHeader()
	if err != nil {
		file.Close()
		return nil, err
	}
	return f, nil
}

func (pbf *Pbf) Close() error {
	return pbf.file.Close()
}

func (pbf *Pbf) parseHeader() error {
	offset, size, header := pbf.nextBlock()
	if header.GetType() != "OSMHeader" {
		panic("invalid block type, expected OSMHeader, got " + header.GetType())
	}
	var err error
	pbf.Header, err = readAndParseHeaderBlock(Block{pbf.Filename, offset, size})
	return err
}

func (pbf *Pbf) nextBlock() (offset int64, size int32, header *osmpbf.BlobHeader) {
	header = pbf.nextBlobHeader()
	size = header.GetDatasize()
	offset = pbf.offset

	pbf.offset += int64(size)
	pbf.file.Seek(pbf.offset, 0)
	return offset, size, header
}

func (pbf *Pbf) BlockPositions() (positions chan Block) {
	positions = make(chan Block, 8)
	go func() {
		for {
			offset, size, header := pbf.nextBlock()
			if size == 0 {
				close(positions)
				pbf.Close()
				return
			}
			if header.GetType() != "OSMData" {
				panic("invalid block type, expected OSMData, got " + header.GetType())
			}
			positions <- Block{pbf.Filename, offset, size}
		}
	}()
	return
}

func (pbf *Pbf) nextBlobHeaderSize() (size int32) {
	pbf.offset += 4
	structs.Read(pbf.file, structs.BigEndian, &size)
	return
}

func (pbf *Pbf) nextBlobHeader() *osmpbf.BlobHeader {
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
