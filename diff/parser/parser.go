package parser

import (
	"compress/gzip"
	"encoding/xml"
	"os"
	"strconv"
	"time"

	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/logging"
)

var log = logging.NewLogger("osc parser")

type DiffElem struct {
	Add  bool
	Mod  bool
	Del  bool
	Node *element.Node
	Way  *element.Way
	Rel  *element.Relation
}

func Parse(diff string) (chan DiffElem, chan error) {
	elems := make(chan DiffElem)
	errc := make(chan error)
	go parse(diff, elems, errc)
	return elems, errc
}

func parse(diff string, elems chan DiffElem, errc chan error) {
	defer close(elems)
	defer close(errc)

	var metaInfo element.MetaInfo
	metaInfo.Reset()

	file, err := os.Open(diff)
	if err != nil {
		errc <- err
		return
	}
	defer file.Close()

	reader, err := gzip.NewReader(file)
	if err != nil {
		errc <- err
		return
	}

	decoder := xml.NewDecoder(reader)

	add := false
	mod := false
	del := false
	tags := make(element.Tags)
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
				add = true
				mod = true
				del = true
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

					if element.Meta.Parse {
						switch attr.Name.Local {
						case "version":
							x, _ := strconv.ParseInt(attr.Value, 10, 32)
							metaInfo.Version = int32(x)
						case "user":
							metaInfo.User = attr.Value
						case "uid":
							x, _ := strconv.ParseInt(attr.Value, 10, 32)
							metaInfo.Uid = int32(x)
						case "changeset":
							metaInfo.Changeset, _ = strconv.ParseInt(attr.Value, 10, 64)
						case "timestamp":
							metaInfo.Timestamp, _ = time.Parse(time.RFC3339, attr.Value)
						}
					}

				}
			case "way":
				for _, attr := range tok.Attr {

					if attr.Name.Local == "id" {
						way.Id, _ = strconv.ParseInt(attr.Value, 10, 64)
					}

					if element.Meta.Parse {
						switch attr.Name.Local {
						case "version":
							x, _ := strconv.ParseInt(attr.Value, 10, 32)
							metaInfo.Version = int32(x)
						case "user":
							metaInfo.User = attr.Value
						case "uid":
							x, _ := strconv.ParseInt(attr.Value, 10, 32)
							metaInfo.Uid = int32(x)
						case "changeset":
							metaInfo.Changeset, _ = strconv.ParseInt(attr.Value, 10, 64)
						case "timestamp":
							metaInfo.Timestamp, _ = time.Parse(time.RFC3339, attr.Value)
						}
					}

				}
			case "relation":
				for _, attr := range tok.Attr {

					if attr.Name.Local == "id" {
						rel.Id, _ = strconv.ParseInt(attr.Value, 10, 64)
					}

					if element.Meta.Parse {
						switch attr.Name.Local {
						case "version":
							x, _ := strconv.ParseInt(attr.Value, 10, 32)
							metaInfo.Version = int32(x)
						case "user":
							metaInfo.User = attr.Value
						case "uid":
							x, _ := strconv.ParseInt(attr.Value, 10, 32)
							metaInfo.Uid = int32(x)
						case "changeset":
							metaInfo.Changeset, _ = strconv.ParseInt(attr.Value, 10, 64)
						case "timestamp":
							metaInfo.Timestamp, _ = time.Parse(time.RFC3339, attr.Value)
						}
					}
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
			var e DiffElem
			switch tok.Name.Local {
			case "node":

				if _, ok := tags["created_by"]; ok && len(tags) == 1 && element.ParseDontAddOnlyCreatedByTag {
					// don't add nodes with only created_by tag to nodes cache
				} else {

					if len(tags) > 0 {

						if element.Meta.Parse {
							tags.AddMetaInfo(metaInfo)
						}

						node.Tags = tags
					}
				}

				e.Node = node
				node = &element.Node{}
				newElem = true

				// reset osm metadata
				metaInfo.Reset()

			case "way":
				if len(tags) > 0 {

					if element.Meta.Parse {
						tags.AddMetaInfo(metaInfo)
					}

					way.Tags = tags
				}
				e.Way = way
				way = &element.Way{}
				newElem = true

				// reset osm metadata
				metaInfo.Reset()

			case "relation":
				if len(tags) > 0 {

					if element.Meta.Parse {
						tags.AddMetaInfo(metaInfo)
					}

					rel.Tags = tags
				}
				e.Rel = rel
				rel = &element.Relation{}
				newElem = true

				// reset osm metadata
				metaInfo.Reset()

			case "osmChange":
				return
			}

			if newElem {
				e.Add = add
				e.Del = del
				e.Mod = mod
				if len(tags) > 0 {
					tags = make(element.Tags)
				}
				newElem = false
				elems <- e
			}
		}
	}

}
