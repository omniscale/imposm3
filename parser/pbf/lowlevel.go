package pbf

import (
	"bytes"
	"compress/zlib"
	structs "encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/omniscale/imposm3/parser/pbf/internal/osmpbf"
)

type block struct {
	filename string
	offset   int64
	size     int32
}

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

func readBlobData(pos block) ([]byte, error) {
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

func readPrimitiveBlock(pos block) *osmpbf.PrimitiveBlock {
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

func readAndParseHeaderBlock(pos block) (*Header, error) {
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

	result := &Header{}
	timestamp := header.GetOsmosisReplicationTimestamp()
	result.Time = time.Unix(timestamp, 0 /* nanoseconds */)
	result.Sequence = header.GetOsmosisReplicationSequenceNumber()
	result.RequiredFeatures = header.RequiredFeatures
	result.OptionalFeatures = header.OptionalFeatures
	return result, nil
}

type pbf struct {
	file     *os.File
	filename string
	offset   int64
	header   *Header
}

type Header struct {
	Time     time.Time
	Sequence int64
	Filename string

	RequiredFeatures []string
	OptionalFeatures []string
}

func open(filename string) (f *pbf, err error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	f = &pbf{filename: filename, file: file}
	err = f.parseHeader()
	if err != nil {
		file.Close()
		return nil, err
	}
	return f, nil
}

func (pbf *pbf) close() error {
	return pbf.file.Close()
}

func (pbf *pbf) parseHeader() error {
	offset, size, header := pbf.nextBlock()
	if header.GetType() != "OSMHeader" {
		panic("invalid block type, expected OSMHeader, got " + header.GetType())
	}
	var err error
	pbf.header, err = readAndParseHeaderBlock(block{pbf.filename, offset, size})
	pbf.header.Filename = pbf.filename
	return err
}

func (pbf *pbf) nextBlock() (offset int64, size int32, header *osmpbf.BlobHeader) {
	header = pbf.nextBlobHeader()
	size = header.GetDatasize()
	offset = pbf.offset

	pbf.offset += int64(size)
	pbf.file.Seek(pbf.offset, 0)
	return offset, size, header
}

func (pbf *pbf) BlockPositions() (positions chan block) {
	positions = make(chan block, 8)
	go func() {
		for {
			offset, size, header := pbf.nextBlock()
			if size == 0 {
				close(positions)
				pbf.close()
				return
			}
			if header.GetType() != "OSMData" {
				panic("invalid block type, expected OSMData, got " + header.GetType())
			}
			positions <- block{pbf.filename, offset, size}
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
