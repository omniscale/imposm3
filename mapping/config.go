package mapping

import (
	"encoding/json"
	"flag"
	"goposm/element"
	"log"
	"os"
)

type Field struct {
	Name string `json:"name"`
	Key  string `json:"key"`
	Type string `json:"type"`
}

type Table struct {
	Name    string
	Type    string              `json:"type"`
	Mapping map[string][]string `json:"mapping"`
	Fields  []*Field            `json:"fields"`
	Filters *Filters            `json:"filters"`
}

type Filters struct {
	ExcludeTags *map[string]string `json:"exclude_tags"`
}

type Tables map[string]*Table

type Mapping struct {
	Tables Tables `json:"tables"`
}

func (t *Table) Mappings() map[string][]string {
	return t.Mapping
}

func (t *Table) ExtraTags() map[string]bool {
	tags := make(map[string]bool)
	for _, field := range t.Fields {
		if field.Key != "" {
			tags[field.Key] = true
		}
	}
	return tags
}

func (m *Mapping) prepare() {
	for name, t := range m.Tables {
		t.Name = name
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

func (m *Mapping) tables(tableType string) map[string]*TableFields {
	result := make(map[string]*TableFields)
	for name, t := range m.Tables {
		if t.Type == tableType {
			result[name] = t.TableFields()
		}
	}
	return result
}

func (m *Mapping) extraTags(tableType string, tags map[string]bool) {
	for _, t := range m.Tables {
		if t.Type != tableType {
			continue
		}
		for key, _ := range t.ExtraTags() {
			tags[key] = true
		}
		if t.Filters != nil && t.Filters.ExcludeTags != nil {
			for key, _ := range *t.Filters.ExcludeTags {
				tags[key] = true
			}
		}
	}
}

func (m *Mapping) elemFilters() map[string][]elemFilter {
	result := make(map[string][]elemFilter)
	for name, t := range m.Tables {
		if t.Filters == nil {
			continue
		}
		if t.Filters.ExcludeTags != nil {
			for filterKey, filterVal := range *t.Filters.ExcludeTags {
				f := func(elem element.OSMElem) bool {
					if v, ok := elem.Tags[filterKey]; ok {
						if filterVal == "__any__" || v == filterVal {
							return false
						}
					}
					return true
				}
				result[name] = append(result[name], f)
			}
		}
	}
	return result
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

func (m *Mapping) RelationTagFilter() *RelationTagFilter {
	mappings := make(map[string]map[string][]string)
	m.mappings("linestring", mappings)
	m.mappings("polygon", mappings)
	tags := make(map[string]bool)
	m.extraTags("linestring", tags)
	m.extraTags("polygon", tags)
	tags["type"] = true // do not filter out type tag
	return &RelationTagFilter{TagFilter{mappings, tags}}
}

func (m *Mapping) PointMatcher() *TagMatcher {
	mappings := make(map[string]map[string][]string)
	m.mappings("point", mappings)
	filters := m.elemFilters()
	return &TagMatcher{mappings, m.tables("point"), filters}
}

func (m *Mapping) LineStringMatcher() *TagMatcher {
	mappings := make(map[string]map[string][]string)
	m.mappings("linestring", mappings)
	filters := m.elemFilters()
	return &TagMatcher{mappings, m.tables("linestring"), filters}
}

func (m *Mapping) PolygonMatcher() *TagMatcher {
	mappings := make(map[string]map[string][]string)
	m.mappings("polygon", mappings)
	filters := m.elemFilters()
	return &TagMatcher{mappings, m.tables("polygon"), filters}
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
			} else if _, ok := f.extraTags[k]; !ok {
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
		if t != "multipolygon" && t != "boundary" && t != "land_area" {
			return false
		}
	} else {
		return false
	}
	f.TagFilter.Filter(tags)
	// always return true here since we found a matching type
	return true
}

type elemFilter func(elem element.OSMElem) bool

type TagMatcher struct {
	mappings map[string]map[string][]string
	tables   map[string]*TableFields
	filters  map[string][]elemFilter
}

type Match struct {
	Key         string
	Value       string
	Table       string
	tableFields *TableFields
}

func (m *Match) Row(elem *element.OSMElem) []interface{} {
	return m.tableFields.MakeRow(elem, *m)
}

func (tagMatcher *TagMatcher) Match(elem element.OSMElem) []Match {
	tables := make(map[string]Match)

	for k, v := range elem.Tags {
		values, ok := tagMatcher.mappings[k]
		if ok {
			if tbls, ok := values["__any__"]; ok {
				for _, t := range tbls {
					tables[t] = Match{k, v, t, tagMatcher.tables[t]}
				}
				continue
			} else if tbls, ok := values[v]; ok {
				for _, t := range tbls {
					tables[t] = Match{k, v, t, tagMatcher.tables[t]}
				}
				continue
			}
		}
	}
	var matches []Match
	for t, match := range tables {
		filters, ok := tagMatcher.filters[t]
		filteredOut := false
		if ok {
			for _, filter := range filters {
				if !filter(elem) {
					filteredOut = true
					break
				}
			}
		}
		if !filteredOut {
			matches = append(matches, match)
		}
	}
	return matches
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

	mapping.prepare()
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
