package mapping

import (
	"encoding/json"
	"goposm/element"
	"os"
)

type Field struct {
	Name string                 `json:"name"`
	Key  string                 `json:"key"`
	Type string                 `json:"type"`
	Args map[string]interface{} `json:"args"`
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

type ElementFilter func(elem *element.OSMElem) bool

type TagTables map[string]map[string][]string

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

func (m *Mapping) mappings(tableType string, mappings TagTables) {
	for name, t := range m.Tables {
		if t.Type != tableType {
			continue
		}
		for key, vals := range t.Mapping {
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

func (m *Mapping) ElementFilters() map[string][]ElementFilter {
	result := make(map[string][]ElementFilter)
	for name, t := range m.Tables {
		if t.Filters == nil {
			continue
		}
		if t.Filters.ExcludeTags != nil {
			for filterKey, filterVal := range *t.Filters.ExcludeTags {
				f := func(elem *element.OSMElem) bool {
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
