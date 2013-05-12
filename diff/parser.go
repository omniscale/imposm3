package main

import (
	"compress/gzip"
	"encoding/xml"
	"flag"
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

	addMod := false
	del := false
	tags := make(map[string]string)
	clear := false

	node := element.Node{}
	way := element.Way{}
	//rel := element.Relation{}

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		switch tok := token.(type) {
		case xml.StartElement:
			switch tok.Name.Local {
			case "add":
				fallthrough
			case "modify":
				addMod = true
				del = false
			case "delete":
				del = true
				addMod = false
			case "node":
				for _, attr := range tok.Attr {
					switch attr.Name.Local {
					case "id":
						node.Id, _ = strconv.ParseInt(attr.Value, 10, 64)
					}
				}
			case "nd":
				for _, attr := range tok.Attr {
					if attr.Name.Local == "ref" {
						ref, _ := strconv.ParseInt(attr.Value, 10, 64)
						way.Refs = append(way.Refs, ref)
					}
				}
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
				log.Println(node, addMod, del)
				clear = true
			case "way":
				way.Tags = tags
				log.Println(way, addMod, del)
				clear = true
				way = element.Way{}
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
