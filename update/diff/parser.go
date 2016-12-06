package diff

import (
	"compress/gzip"
	"encoding/xml"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/logging"
)

var log = logging.NewLogger("osc parser")

type Element struct {
	Add  bool
	Mod  bool
	Del  bool
	Node *element.Node
	Way  *element.Way
	Rel  *element.Relation
}

// Parser is a stream based parser for OSM diff files (.osc).
// Parsing is handled in a background goroutine.
type Parser struct {
	reader   io.Reader
	elems    chan Element
	errc     chan error
	metadata bool
	running  bool
	onClose  func() error
}

// SetWithMetadata enables parsing of metadata
func (p *Parser) SetWithMetadata(metadata bool) {
	p.metadata = metadata
}

// Next returns the next Element of the .osc file.
// Returns io.EOF and an empty Element if the parser
// reached the end of the .osc file.
func (p *Parser) Next() (Element, error) {
	if !p.running {
		p.running = true
		go parse(p.reader, p.elems, p.errc, p.metadata)
	}
	select {
	case elem, ok := <-p.elems:
		if !ok {
			p.elems = nil
		} else {
			return elem, nil
		}
	case err, ok := <-p.errc:
		if !ok {
			p.errc = nil
		} else {
			if p.onClose != nil {
				p.onClose()
				p.onClose = nil
			}
			return Element{}, err
		}
	}
	if p.onClose != nil {
		err := p.onClose()
		p.onClose = nil
		return Element{}, err
	}
	return Element{}, nil
}

// NewParser returns a parser from an io.Reader
func NewParser(r io.Reader) *Parser {
	elems := make(chan Element)
	errc := make(chan error)
	return &Parser{reader: r, elems: elems, errc: errc}
}

// NewOscGzParser returns a parser from a .osc.gz file
func NewOscGzParser(fname string) (*Parser, error) {
	file, err := os.Open(fname)
	if err != nil {
		return nil, err
	}

	reader, err := gzip.NewReader(file)
	if err != nil {
		file.Close()
		return nil, err
	}

	elems := make(chan Element)
	errc := make(chan error)
	return &Parser{reader: reader, elems: elems, errc: errc, onClose: file.Close}, nil
}

func parse(reader io.Reader, elems chan Element, errc chan error, metadata bool) {
	defer close(elems)
	defer close(errc)

	decoder := xml.NewDecoder(reader)

	add := false
	mod := false
	del := false
	tags := make(map[string]string)
	newElem := false

	node := &element.Node{}
	way := &element.Way{}
	rel := &element.Relation{}

NextToken:
	for {
		token, err := decoder.Token()
		if err != nil {
			errc <- err
			return
		}

		switch tok := token.(type) {
		case xml.StartElement:
			switch tok.Name.Local {
			case "create":
				add = true
				mod = false
				del = false
			case "modify":
				add = false
				mod = true
				del = false
			case "delete":
				add = false
				mod = false
				del = true
			case "node":
				for _, attr := range tok.Attr {
					switch attr.Name.Local {
					case "id":
						node.Id, _ = strconv.ParseInt(attr.Value, 10, 64)
					case "lat":
						node.Lat, _ = strconv.ParseFloat(attr.Value, 64)
					case "lon":
						node.Long, _ = strconv.ParseFloat(attr.Value, 64)
					}
				}
				if metadata {
					setElemMetadata(tok.Attr, &node.OSMElem)
				}
			case "way":
				for _, attr := range tok.Attr {
					if attr.Name.Local == "id" {
						way.Id, _ = strconv.ParseInt(attr.Value, 10, 64)
					}
				}
				if metadata {
					setElemMetadata(tok.Attr, &way.OSMElem)
				}
			case "relation":
				for _, attr := range tok.Attr {
					if attr.Name.Local == "id" {
						rel.Id, _ = strconv.ParseInt(attr.Value, 10, 64)
					}
				}
				if metadata {
					setElemMetadata(tok.Attr, &rel.OSMElem)
				}
			case "nd":
				for _, attr := range tok.Attr {
					if attr.Name.Local == "ref" {
						ref, _ := strconv.ParseInt(attr.Value, 10, 64)
						way.Refs = append(way.Refs, ref)
					}
				}
			case "member":
				member := element.Member{}
				for _, attr := range tok.Attr {
					switch attr.Name.Local {
					case "type":
						var ok bool
						member.Type, ok = element.MemberTypeValues[attr.Value]
						if !ok {
							// ignore unknown member types
							continue NextToken
						}
					case "role":
						member.Role = attr.Value
					case "ref":
						var err error
						member.Id, err = strconv.ParseInt(attr.Value, 10, 64)
						if err != nil {
							// ignore invalid ref
							continue NextToken
						}
					}
				}
				rel.Members = append(rel.Members, member)
			case "tag":
				var k, v string
				for _, attr := range tok.Attr {
					if attr.Name.Local == "k" {
						k = attr.Value
					} else if attr.Name.Local == "v" {
						v = attr.Value
					}
				}
				tags[k] = v
			case "osmChange":
				// pass
			default:
				log.Warn("unhandled XML tag ", tok.Name.Local, " in OSC")
			}
		case xml.EndElement:
			var e Element
			switch tok.Name.Local {
			case "node":
				if len(tags) > 0 {
					node.Tags = tags
				}
				e.Node = node
				node = &element.Node{}
				newElem = true
			case "way":
				if len(tags) > 0 {
					way.Tags = tags
				}
				e.Way = way
				way = &element.Way{}
				newElem = true
			case "relation":
				if len(tags) > 0 {
					rel.Tags = tags
				}
				e.Rel = rel
				rel = &element.Relation{}
				newElem = true
			case "osmChange":
				errc <- io.EOF
				return
			}

			if newElem {
				e.Add = add
				e.Del = del
				e.Mod = mod
				if len(tags) > 0 {
					tags = make(map[string]string)
				}
				newElem = false
				elems <- e
			}
		}
	}
}

func setElemMetadata(attrs []xml.Attr, elem *element.OSMElem) {
	elem.Metadata = &element.Metadata{}
	for _, attr := range attrs {
		switch attr.Name.Local {
		case "version":
			v, _ := strconv.ParseInt(attr.Value, 10, 64)
			elem.Metadata.Version = int(v)
		case "uid":
			v, _ := strconv.ParseInt(attr.Value, 10, 64)
			elem.Metadata.UserId = int(v)
		case "user":
			elem.Metadata.UserName = attr.Value
		case "changeset":
			v, _ := strconv.ParseInt(attr.Value, 10, 64)
			elem.Metadata.Changeset = int(v)
		case "timestamp":
			elem.Metadata.Timestamp, _ = time.Parse(time.RFC3339, attr.Value)
		}
	}
}
