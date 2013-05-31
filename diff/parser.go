package main

import (
	"compress/gzip"
	"encoding/xml"
	"flag"
	"fmt"
	"goposm/element"
	"io"
	"log"
	"os"
	"strconv"
)

func main() {
	flag.Parse()
	file, err := os.Open(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}

	reader, err := gzip.NewReader(file)
	if err != nil {
		log.Fatal(err)
	}
	decoder := xml.NewDecoder(reader)

	add := false
	del := false
	tags := make(map[string]string)
	clear := false

	node := element.Node{}
	way := element.Way{}
	rel := element.Relation{}

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

	TokenSwitch:
		switch tok := token.(type) {
		case xml.StartElement:
			switch tok.Name.Local {
			case "add":
				add = true
				del = false
			case "modify":
				add = true
				del = true
			case "delete":
				del = true
				add = false
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
			case "way":
				for _, attr := range tok.Attr {
					if attr.Name.Local == "id" {
						way.Id, _ = strconv.ParseInt(attr.Value, 10, 64)
					}
				}
			case "relation":
				for _, attr := range tok.Attr {
					if attr.Name.Local == "id" {
						rel.Id, _ = strconv.ParseInt(attr.Value, 10, 64)
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
							break TokenSwitch
						}
					case "role":
						member.Role = attr.Value
					case "ref":
						var err error
						member.Id, err = strconv.ParseInt(attr.Value, 10, 64)
						if err != nil {
							// ignore invalid ref
							break TokenSwitch
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
			}
		case xml.EndElement:
			switch tok.Name.Local {
			case "node":
				node.Tags = tags
				// fmt.Println(node, add, del)
				clear = true
			case "way":
				way.Tags = tags
				fmt.Println(way, add, del)
				clear = true
				way = element.Way{}
			case "rel":
				rel.Tags = tags
				fmt.Println(rel, add, del)
				clear = true
				rel = element.Relation{}
			}

			if clear {
				if len(tags) > 0 {
					tags = make(map[string]string)
				}
				clear = false
			}
		}
	}

}
