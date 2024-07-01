package pbf

import (
	"bytes"
	"compress/zlib"
	structs "encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/omniscale/go-osm/parser/pbf/internal/osmpbf"
)

var supportedFeatured = map[string]bool{"OsmSchema-V0.6": true, "DenseNodes": true}

// decodeRawBlob decodes Blob PBF messages and returns either the raw bytes or
// the uncompressed zlib_data bytes. The result can contain encoded HeaderBlock
// or PrimitiveBlock PBF messages.
func decodeRawBlob(raw []byte) ([]byte, error) {
	blob := &osmpbf.Blob{}

	err := proto.Unmarshal(raw, blob)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling blob: %w", err)
	}

	// pbf contains (uncompressed) raw or zlibdata
	b := blob.GetRaw()
	if b == nil {
		buf := bytes.NewBuffer(blob.GetZlibData())
		r, err := zlib.NewReader(buf)
		if err != nil {
			return nil, fmt.Errorf("start uncompressing ZLibData: %w", err)
		}
		b = make([]byte, blob.GetRawSize())
		_, err = io.ReadFull(r, b)
		if err != nil {
			return nil, fmt.Errorf("uncompressing ZLibData: %w", err)
		}
	}
	return b, nil
}

func decodePrimitiveBlock(blob []byte) (*osmpbf.PrimitiveBlock, error) {
	b, err := decodeRawBlob(blob)
	if err != nil {
		return nil, fmt.Errorf("decoding raw blob: %w", err)
	}
	block := &osmpbf.PrimitiveBlock{}
	if err = proto.Unmarshal(b, block); err != nil {
		return nil, fmt.Errorf("unmarshaling PrimitiveBlock: %w", err)
	}
	return block, nil
}

func decodeHeaderBlock(blob []byte) (*Header, error) {
	b, err := decodeRawBlob(blob)
	if err != nil {
		return nil, err
	}

	header := &osmpbf.HeaderBlock{}
	if err := proto.Unmarshal(b, header); err != nil {
		return nil, err
	}

	for _, feature := range header.RequiredFeatures {
		if supportedFeatured[feature] != true {
			return nil, fmt.Errorf("cannot parse file, feature %v not supported: %w", feature, err)
		}
	}

	result := &Header{}
	timestamp := header.GetOsmosisReplicationTimestamp()
	if timestamp != 0 {
		// keep result.Time zero if timestamp is 0
		result.Time = time.Unix(timestamp, 0)
	}
	result.Sequence = header.GetOsmosisReplicationSequenceNumber()
	result.RequiredFeatures = header.RequiredFeatures
	result.OptionalFeatures = header.OptionalFeatures
	return result, nil
}

type Header struct {
	Time     time.Time
	Sequence int64

	RequiredFeatures []string
	OptionalFeatures []string
}

func parseHeader(r io.Reader) (*Header, error) {
	blockHeader, data, err := nextBlock(r)
	if err != nil {
		return nil, fmt.Errorf("reading header: %w", err)
	}
	if blockHeader.GetType() != "OSMHeader" {
		return nil, errors.New("invalid block type, expected OSMHeader, got " + blockHeader.GetType())
	}
	header, err := decodeHeaderBlock(data)
	return header, err
}

func nextBlock(r io.Reader) (*osmpbf.BlobHeader, []byte, error) {
	header, err := nextBlobHeader(r)
	if err == io.EOF {
		return nil, nil, err
	}
	if err != nil {
		return nil, nil, fmt.Errorf("reading next block header: %w", err)
	}
	size := header.GetDatasize()

	data := make([]byte, size)
	n, err := io.ReadFull(r, data)
	if err != nil {
		return nil, nil, fmt.Errorf("reading next block: %w", err)
	}
	if n != int(size) {
		return nil, nil, fmt.Errorf("reading next block, only got %d bytes instead of %d", n, size)
	}
	return header, data, nil
}

func nextBlobHeader(r io.Reader) (*osmpbf.BlobHeader, error) {
	var size int32
	err := structs.Read(r, structs.BigEndian, &size)
	if err == io.EOF {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("reading header size: %w", err)
	}

	var blobHeader = &osmpbf.BlobHeader{}

	data := make([]byte, size)
	n, err := io.ReadFull(r, data)
	if err != nil {
		return nil, fmt.Errorf("reading blob header: %w", err)
	}
	if n != int(size) {
		return nil, fmt.Errorf("reading blob header, only got %d bytes instead of %d", n, size)
	}

	err = proto.Unmarshal(data, blobHeader)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling header: %w", err)
	}

	return blobHeader, nil
}
