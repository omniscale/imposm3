package config

import (
	"encoding/json"
	"flag"
	"log"
	"os"
)

type Field struct {
	Key  string `json:"key"`
	Type string `json:"type"`
}

type Table struct {
	Type    string              `json:"type"`
	Mapping map[string][]string `json:"mapping"`
	Fields  map[string]*Field   `json:"fields"`
}

type Tables map[string]Table

type Mapping struct {
	Tables Tables `json:"tables"`
}

func (t *Table) FillFieldKeys() {
	for key, field := range t.Fields {
		if field.Key == "" {
			field.Key = key
		}
	}
}

func (t *Table) Mappings() map[string][]string {
	return t.Mapping
}

func (t *Table) ExtraTags() map[string]bool {
	tags := make(map[string]bool)
	for _, field := range t.Fields {
		tags[field.Key] = true
	}
	return tags
}

func (m *Mapping) FillFieldKeys() {
	for _, t := range m.Tables {
		t.FillFieldKeys()
	}
}

func (m *Mapping) mappings(tableType string, mappings map[string]map[string][]string) {
	for name, t := range m.Tables {
		if t.Type != tableType {
			continue
		}
		for key, vals := range t.Mappings() {
			for _, v := range vals {
				vals, ok := mappings[key]
				if ok {
					vals[v] = append(vals[v], name)
				} else {
					mappings[key] = make(map[string][]string)
					mappings[key][v] = append(mappings[key][v], name)
				}
			}
		}
	}
}

func (m *Mapping) extraTags(tableType string, tags map[string]bool) {
	for _, t := range m.Tables {
		if t.Type != tableType {
			continue
		}
		for key, _ := range t.ExtraTags() {
			tags[key] = true
		}
	}
}

func (m *Mapping) NodeTagFilter() *TagFilter {
	mappings := make(map[string]map[string][]string)
	m.mappings("point", mappings)
	tags := make(map[string]bool)
	m.extraTags("point", tags)
	return &TagFilter{mappings, tags}
}

func (m *Mapping) WayTagFilter() *TagFilter {
	mappings := make(map[string]map[string][]string)
	m.mappings("linestring", mappings)
	m.mappings("polygon", mappings)
	tags := make(map[string]bool)
	m.extraTags("linestring", tags)
	m.extraTags("polygon", tags)
	return &TagFilter{mappings, tags}
}

func (m *Mapping) RelationTagFilter() *TagFilter {
	mappings := make(map[string]map[string][]string)
	m.mappings("linestring", mappings)
	m.mappings("polygon", mappings)
	tags := make(map[string]bool)
	m.extraTags("linestring", tags)
	m.extraTags("polygon", tags)
	return &TagFilter{mappings, tags}
}

type TagFilter struct {
	mappings  map[string]map[string][]string
	extraTags map[string]bool
}

type RelationTagFilter struct {
	TagFilter
}

func (f *TagFilter) Filter(tags map[string]string) bool {
	foundMapping := false
	for k, v := range tags {
		values, ok := f.mappings[k]
		if ok {
			if _, ok := values["__any__"]; ok {
				foundMapping = true
				continue
			} else if _, ok := values[v]; ok {
				foundMapping = true
				continue
			} else {
				delete(tags, k)
			}
		} else if _, ok := f.extraTags[k]; !ok {
			delete(tags, k)
		}
	}
	if foundMapping {
		return true
	} else {
		return false
	}
}

func (f *RelationTagFilter) Filter(tags map[string]string) bool {
	if t, ok := tags["type"]; ok {
		if t != "multipolygon" || t != "boundary" || t != "land_area" {
			return false
		}
	} else {
		return false
	}
	return f.TagFilter.Filter(tags)
}

func (f *TagFilter) Tables(tags map[string]string) []string {
	tables := make(map[string]bool)

	for k, v := range tags {
		values, ok := f.mappings[k]
		if ok {
			if tbls, ok := values["__any__"]; ok {
				for _, t := range tbls {
					tables[t] = true
				}
				continue
			} else if tbls, ok := values[v]; ok {
				for _, t := range tbls {
					tables[t] = true
				}
				continue
			}
		}
	}
	var tableNames []string
	for name, _ := range tables {
		tableNames = append(tableNames, name)
	}
	return tableNames
}

func NewMapping(filename string) (*Mapping, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(f)

	mapping := Mapping{}
	err = decoder.Decode(&mapping)
	if err != nil {
		return nil, err
	}

	mapping.FillFieldKeys()
	return &mapping, nil
}

func main() {
	// 	data := `
	// {
	//     "tables": {
	//         "roads": {
	//             "mapping": {
	//                 "highway": [
	//                     "motorway",
	//                     "motorway_link",
	//                     "trunk",
	//                     "trunk_link"
	//                 ]
	//             },
	//             "fields": {
	//                 "tunnel": {"type": "bool", "key": "tunnel"},
	//                 "bridge": {"type": "bool"},
	//                 "oneway": {"type": "direction"},
	//                 "ref": {"type": "string"},
	//                 "z_order": {"type": "wayzorder", "key": "NONE"}
	//             }
	//         }
	//     }
	// }
	// `

	// t := Table{map[string][]string{"highway": {"motorway", "trunk"}}}
	// b, err := json.Marshal(t)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// log.Println(string(b))

	flag.Parse()

	mapping, err := NewMapping(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
	// log.Println(mapping.Mappings("point"))
	// log.Println(mapping.ExtraTags("point"))
	log.Println(mapping.NodeTagFilter())
	log.Println(mapping.WayTagFilter())
	log.Println(mapping.RelationTagFilter())

	// log.Println(mapping)

	// b, err := json.MarshalIndent(mapping, "", "   ")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// log.Println(string(b))
}
