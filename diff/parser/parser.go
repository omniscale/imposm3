package parser

import (
	"compress/gzip"
	"encoding/xml"
	"os"
	"strconv"

	"github.com/omniscale/imposm3/config"
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

	var user string
	var uid int64
	var changeset int64
	var version int64
	var timestamp string

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

					if config.ParseMetadata {
						switch attr.Name.Local {
						case "id":
							// reset values
							user = ""
							uid = 0
							changeset = 0
							version = 0
							timestamp = ""
						case "version":
							version, _ = strconv.ParseInt(attr.Value, 10, 64)
						case "user":
							user = attr.Value
						case "uid":
							uid, _ = strconv.ParseInt(attr.Value, 10, 64)
						case "changeset":
							changeset, _ = strconv.ParseInt(attr.Value, 10, 64)
						case "timestamp":
							timestamp = attr.Value
						}
					}

				}
			case "way":
				for _, attr := range tok.Attr {

					if attr.Name.Local == "id" {
						way.Id, _ = strconv.ParseInt(attr.Value, 10, 64)
					}

					if config.ParseMetadata {
						switch attr.Name.Local {
						case "id":
							// reset values
							user = ""
							uid = 0
							changeset = 0
							version = 0
							timestamp = ""
						case "version":
							version, _ = strconv.ParseInt(attr.Value, 10, 64)
						case "user":
							user = attr.Value
						case "uid":
							uid, _ = strconv.ParseInt(attr.Value, 10, 64)
						case "changeset":
							changeset, _ = strconv.ParseInt(attr.Value, 10, 64)
						case "timestamp":
							timestamp = attr.Value
						}
					}

				}
			case "relation":
				for _, attr := range tok.Attr {

					if attr.Name.Local == "id" {
						rel.Id, _ = strconv.ParseInt(attr.Value, 10, 64)
					}

					if config.ParseMetadata {
						switch attr.Name.Local {
						case "id":
							// reset values
							user = ""
							uid = 0
							changeset = 0
							version = 0
							timestamp = ""
						case "version":
							version, _ = strconv.ParseInt(attr.Value, 10, 64)
						case "user":
							user = attr.Value
						case "uid":
							uid, _ = strconv.ParseInt(attr.Value, 10, 64)
						case "changeset":
							changeset, _ = strconv.ParseInt(attr.Value, 10, 64)
						case "timestamp":
							timestamp = attr.Value
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
				if len(tags) > 0 {

					if config.ParseMetadata {

						if config.ParseMetadataVarChangeset {
							tags[config.ParseMetadataKeynameChangeset] = strconv.FormatInt(changeset, 10)
						}
						if config.ParseMetadataVarVersion {
							tags[config.ParseMetadataKeynameVersion] = strconv.FormatInt(version, 10)
						}
						if config.ParseMetadataVarUser {
							tags[config.ParseMetadataKeynameUser] = user
						}
						if config.ParseMetadataVarUid {
							tags[config.ParseMetadataKeynameUid] = strconv.FormatInt(uid, 10)
						}
						if config.ParseMetadataVarTimestamp {
							tags[config.ParseMetadataKeynameTimestamp] = timestamp
						}
					}

					node.Tags = tags
				}
				e.Node = node
				node = &element.Node{}
				newElem = true

			case "way":
				if len(tags) > 0 {

					if config.ParseMetadata {

						if config.ParseMetadataVarChangeset {
							tags[config.ParseMetadataKeynameChangeset] = strconv.FormatInt(changeset, 10)
						}
						if config.ParseMetadataVarVersion {
							tags[config.ParseMetadataKeynameVersion] = strconv.FormatInt(version, 10)
						}
						if config.ParseMetadataVarUser {
							tags[config.ParseMetadataKeynameUser] = user
						}
						if config.ParseMetadataVarUid {
							tags[config.ParseMetadataKeynameUid] = strconv.FormatInt(uid, 10)
						}
						if config.ParseMetadataVarTimestamp {
							tags[config.ParseMetadataKeynameTimestamp] = timestamp
						}
					}

					way.Tags = tags
				}
				e.Way = way
				way = &element.Way{}
				newElem = true

			case "relation":
				if len(tags) > 0 {

					if config.ParseMetadata {

						if config.ParseMetadataVarChangeset {
							tags[config.ParseMetadataKeynameChangeset] = strconv.FormatInt(changeset, 10)
						}
						if config.ParseMetadataVarVersion {
							tags[config.ParseMetadataKeynameVersion] = strconv.FormatInt(version, 10)
						}
						if config.ParseMetadataVarUser {
							tags[config.ParseMetadataKeynameUser] = user
						}
						if config.ParseMetadataVarUid {
							tags[config.ParseMetadataKeynameUid] = strconv.FormatInt(uid, 10)
						}
						if config.ParseMetadataVarTimestamp {
							tags[config.ParseMetadataKeynameTimestamp] = timestamp
						}
					}

					rel.Tags = tags
				}
				e.Rel = rel
				rel = &element.Relation{}
				newElem = true

			case "osmChange":
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
