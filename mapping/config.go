package mapping

import (
	"encoding/json"
	"errors"
	"imposm3/element"
	"os"
)

type Field struct {
	Name string                 `json:"name"`
	Key  string                 `json:"key"`
	Type string                 `json:"type"`
	Args map[string]interface{} `json:"args"`
}

type Table struct {
	Name     string
	Type     TableType             `json:"type"`
	Mapping  map[string][]string   `json:"mapping"`
	Mappings map[string]SubMapping `json:"mappings"`
	Fields   []*Field              `json:"fields"`
	Filters  *Filters              `json:"filters"`
}

type SubMapping struct {
	Mapping map[string][]string
}

type GeneralizedTable struct {
	Name            string
	SourceTableName string  `json:"source"`
	Tolerance       float64 `json:"tolerance"`
	SqlFilter       string  `json:"sql_filter"`
}

type Filters struct {
	ExcludeTags *[][2]string `json:"exclude_tags"`
}

type Tables map[string]*Table

type GeneralizedTables map[string]*GeneralizedTable

type Mapping struct {
	Tables            Tables            `json:"tables"`
	GeneralizedTables GeneralizedTables `json:"generalized_tables"`
}

type ElementFilter func(tags *element.Tags) bool

type TagTables map[string]map[string][]DestTable

type DestTable struct {
	Name       string
	SubMapping string
}

type TableType string

const (
	PolygonTable    TableType = "polygon"
	LineStringTable TableType = "linestring"
	PointTable      TableType = "point"
)

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

	err = mapping.prepare()
	if err != nil {
		return nil, err
	}
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

func (m *Mapping) prepare() error {
	for name, t := range m.Tables {
		t.Name = name
	}
	for name, t := range m.Tables {
		switch t.Type {
		case "":
			return errors.New("missing table type for table " + name)
		case "point":
		case "linestring":
		case "polygon":
		default:
			return errors.New("unknown type " + string(t.Type) + " for table " + name)
		}
	}
	for name, t := range m.GeneralizedTables {
		t.Name = name
	}
	return nil
}

func (tt TagTables) addFromMapping(mapping map[string][]string, table DestTable) {
	for key, vals := range mapping {
		for _, v := range vals {
			vals, ok := tt[key]
			if ok {
				vals[v] = append(vals[v], table)
			} else {
				tt[key] = make(map[string][]DestTable)
				tt[key][v] = append(tt[key][v], table)
			}
		}
	}

}

func (m *Mapping) mappings(tableType TableType, mappings TagTables) {
	for name, t := range m.Tables {
		if t.Type != tableType {
			continue
		}
		mappings.addFromMapping(t.Mapping, DestTable{name, ""})

		for subMappingName, subMapping := range t.Mappings {
			mappings.addFromMapping(subMapping.Mapping, DestTable{name, subMappingName})
		}
	}
}

func (m *Mapping) tables(tableType TableType) map[string]*TableFields {
	result := make(map[string]*TableFields)
	for name, t := range m.Tables {
		if t.Type == tableType {
			result[name] = t.TableFields()
		}
	}
	return result
}

func (m *Mapping) extraTags(tableType TableType, tags map[string]bool) {
	for _, t := range m.Tables {
		if t.Type != tableType {
			continue
		}
		for key, _ := range t.ExtraTags() {
			tags[key] = true
		}
		if t.Filters != nil && t.Filters.ExcludeTags != nil {
			for _, keyVal := range *t.Filters.ExcludeTags {
				tags[keyVal[0]] = true
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
			for _, filterKeyVal := range *t.Filters.ExcludeTags {
				f := func(tags *element.Tags) bool {
					if v, ok := (*tags)[filterKeyVal[0]]; ok {
						if filterKeyVal[1] == "__any__" || v == filterKeyVal[1] {
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
