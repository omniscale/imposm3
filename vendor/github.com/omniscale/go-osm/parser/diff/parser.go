package diff

import (
	"compress/gzip"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/omniscale/go-osm"
)

// Parser is a stream based parser for OSM diff files (.osc).
type Parser struct {
	reader io.Reader
	conf   Config
	err    error
}

type Config struct {
	// IncludeMetadata indicates whether metadata like timestamps, versions and
	// user names should be parsed.
	IncludeMetadata bool

	// Diffs specifies the destination for parsed diff elements.
	Diffs chan osm.Diff

	// KeepOpen specifies whether the destination channel should be keept open
	// after Parse(). By default, the Elements channel is closed after Parse().
	KeepOpen bool
}

// New creates a new parser for the provided input. Config specifies the destinations for the parsed elements.
func New(r io.Reader, conf Config) *Parser {
	return &Parser{reader: r, conf: conf}
}

// NewGZIP returns a parser from a GZIP compressed io.Reader
func NewGZIP(r io.Reader, conf Config) (*Parser, error) {
	r, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	return New(r, conf), nil
}

// Error returns the first error that occurred during Parse calls.
func (p *Parser) Error() error {
	return p.err
}

func (p *Parser) Parse(ctx context.Context) (err error) {
	if p.err != nil {
		return err
	}

	defer func() {
		if err != nil {
			p.err = err
		}
	}()

	if !p.conf.KeepOpen {
		defer func() {
			if p.conf.Diffs != nil {
				close(p.conf.Diffs)
			}
		}()
	}
	decoder := xml.NewDecoder(p.reader)

	add := false
	mod := false
	del := false
	tags := make(map[string]string)
	newElem := false

	node := &osm.Node{}
	way := &osm.Way{}
	rel := &osm.Relation{}

NextToken:
	for {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("decoding next XML token: %w", err)
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
						node.ID, _ = strconv.ParseInt(attr.Value, 10, 64)
					case "lat":
						node.Lat, _ = strconv.ParseFloat(attr.Value, 64)
					case "lon":
						node.Long, _ = strconv.ParseFloat(attr.Value, 64)
					}
				}
				if p.conf.IncludeMetadata {
					setElemMetadata(tok.Attr, &node.Element)
				}
			case "way":
				for _, attr := range tok.Attr {
					if attr.Name.Local == "id" {
						way.ID, _ = strconv.ParseInt(attr.Value, 10, 64)
					}
				}
				if p.conf.IncludeMetadata {
					setElemMetadata(tok.Attr, &way.Element)
				}
			case "relation":
				for _, attr := range tok.Attr {
					if attr.Name.Local == "id" {
						rel.ID, _ = strconv.ParseInt(attr.Value, 10, 64)
					}
				}
				if p.conf.IncludeMetadata {
					setElemMetadata(tok.Attr, &rel.Element)
				}
			case "nd":
				for _, attr := range tok.Attr {
					if attr.Name.Local == "ref" {
						ref, _ := strconv.ParseInt(attr.Value, 10, 64)
						way.Refs = append(way.Refs, ref)
					}
				}
			case "member":
				member := osm.Member{}
				for _, attr := range tok.Attr {
					switch attr.Name.Local {
					case "type":
						var ok bool
						member.Type, ok = memberTypeValues[attr.Value]
						if !ok {
							// ignore unknown member types
							continue NextToken
						}
					case "role":
						member.Role = attr.Value
					case "ref":
						var err error
						member.ID, err = strconv.ParseInt(attr.Value, 10, 64)
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
				// unhandled XML tag, pass
			}
		case xml.EndElement:
			var e osm.Diff
			switch tok.Name.Local {
			case "node":
				if len(tags) > 0 {
					node.Tags = tags
				}
				e.Node = node
				node = &osm.Node{}
				newElem = true
			case "way":
				if len(tags) > 0 {
					way.Tags = tags
				}
				e.Way = way
				way = &osm.Way{}
				newElem = true
			case "relation":
				if len(tags) > 0 {
					rel.Tags = tags
				}
				e.Rel = rel
				rel = &osm.Relation{}
				newElem = true
			case "osmChange":
				// EOF
				return nil
			}

			if newElem {
				e.Create = add
				e.Delete = del
				e.Modify = mod
				if len(tags) > 0 {
					tags = make(map[string]string)
				}
				newElem = false
				select {
				case <-ctx.Done():
				case p.conf.Diffs <- e:
				}
			}
		}
	}

	return nil
}

func setElemMetadata(attrs []xml.Attr, elem *osm.Element) {
	elem.Metadata = &osm.Metadata{}
	for _, attr := range attrs {
		switch attr.Name.Local {
		case "version":
			v, _ := strconv.ParseInt(attr.Value, 10, 64)
			elem.Metadata.Version = int32(v)
		case "uid":
			v, _ := strconv.ParseInt(attr.Value, 10, 64)
			elem.Metadata.UserID = int32(v)
		case "user":
			elem.Metadata.UserName = attr.Value
		case "changeset":
			v, _ := strconv.ParseInt(attr.Value, 10, 64)
			elem.Metadata.Changeset = v
		case "timestamp":
			elem.Metadata.Timestamp, _ = time.Parse(time.RFC3339, attr.Value)
		}
	}
}

var memberTypeValues = map[string]osm.MemberType{
	"node":     osm.NodeMember,
	"way":      osm.WayMember,
	"relation": osm.RelationMember,
}
