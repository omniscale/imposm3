package mapping

import (
	"errors"
	"io/ioutil"

	"github.com/omniscale/imposm3/element"

	"gopkg.in/yaml.v2"
)

type Field struct {
	Name string                 `yaml:"name"`
	Key  Key                    `yaml:"key"`
	Keys []Key                  `yaml:"keys"`
	Type string                 `yaml:"type"`
	Args map[string]interface{} `yaml:"args"`
}

type Table struct {
	Name         string
	Type         TableType             `yaml:"type"`
	Mapping      map[Key][]Value       `yaml:"mapping"`
	Mappings     map[string]SubMapping `yaml:"mappings"`
	TypeMappings TypeMappings          `yaml:"type_mappings"`
	Fields       []*Field              `yaml:"columns"` // TODO rename Fields internaly to Columns
	OldFields    []*Field              `yaml:"fields"`
	Filters      *Filters              `yaml:"filters"`
}

type GeneralizedTable struct {
	Name            string
	SourceTableName string  `yaml:"source"`
	Tolerance       float64 `yaml:"tolerance"`
	SqlFilter       string  `yaml:"sql_filter"`
}

type Filters struct {
	ExcludeTags *[][]string `yaml:"exclude_tags"`
}

type Tables map[string]*Table

type GeneralizedTables map[string]*GeneralizedTable

type Mapping struct {
	Tables            Tables            `yaml:"tables"`
	GeneralizedTables GeneralizedTables `yaml:"generalized_tables"`
	Tags              Tags              `yaml:"tags"`
	// SingleIdSpace mangles the overlapping node/way/relation IDs
	// to be unique (nodes positive, ways negative, relations negative -1e17)
	SingleIdSpace 	  bool 				`yaml:"use_single_id_space"`
}

type Tags struct {
	LoadAll bool  `yaml:"load_all"`
	Exclude []Key `yaml:"exclude"`
}

type SubMapping struct {
	Mapping map[Key][]Value
}

type TypeMappings struct {
	Points      map[Key][]Value `yaml:"points"`
	LineStrings map[Key][]Value `yaml:"linestrings"`
	Polygons    map[Key][]Value `yaml:"polygons"`
}

type ElementFilter func(tags *element.Tags) bool

type TagTables map[Key]map[Value][]DestTable

type DestTable struct {
	Name       string
	SubMapping string
}

type TableType string

func (tt *TableType) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case "":
		return errors.New("missing table type")
	case `"point"`:
		*tt = PointTable
	case `"linestring"`:
		*tt = LineStringTable
	case `"polygon"`:
		*tt = PolygonTable
	case `"geometry"`:
		*tt = GeometryTable
	default:
		return errors.New("unknown type " + string(data))
	}
	return nil
}

const (
	PolygonTable    TableType = "polygon"
	LineStringTable TableType = "linestring"
	PointTable      TableType = "point"
	GeometryTable   TableType = "geometry"
)

func NewMapping(filename string) (*Mapping, error) {
	f, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	mapping := Mapping{}
	err = yaml.Unmarshal(f, &mapping)
	if err != nil {
		return nil, err
	}

	err = mapping.prepare()
	if err != nil {
		return nil, err
	}
	return &mapping, nil
}

func (t *Table) ExtraTags() map[Key]bool {
	tags := make(map[Key]bool)
	for _, field := range t.Fields {
		if field.Key != "" {
			tags[field.Key] = true
		}
		for _, k := range field.Keys {
			tags[k] = true
		}
	}
	return tags
}

func (m *Mapping) prepare() error {
	for name, t := range m.Tables {
		t.Name = name
		if t.OldFields != nil {
			// todo deprecate 'fields'
			t.Fields = t.OldFields
		}
	}

	for name, t := range m.GeneralizedTables {
		t.Name = name
	}
	return nil
}

func (tt TagTables) addFromMapping(mapping map[Key][]Value, table DestTable) {
	for key, vals := range mapping {
		for _, v := range vals {
			vals, ok := tt[key]
			if ok {
				vals[v] = append(vals[v], table)
			} else {
				tt[key] = make(map[Value][]DestTable)
				tt[key][v] = append(tt[key][v], table)
			}
		}
	}
}

func (m *Mapping) mappings(tableType TableType, mappings TagTables) {
	for name, t := range m.Tables {
		if t.Type != GeometryTable && t.Type != tableType {
			continue
		}
		mappings.addFromMapping(t.Mapping, DestTable{name, ""})

		for subMappingName, subMapping := range t.Mappings {
			mappings.addFromMapping(subMapping.Mapping, DestTable{name, subMappingName})
		}

		switch tableType {
		case PointTable:
			mappings.addFromMapping(t.TypeMappings.Points, DestTable{name, ""})
		case LineStringTable:
			mappings.addFromMapping(t.TypeMappings.LineStrings, DestTable{name, ""})
		case PolygonTable:
			mappings.addFromMapping(t.TypeMappings.Polygons, DestTable{name, ""})
		}
	}
}

func (m *Mapping) tables(tableType TableType) map[string]*TableFields {
	result := make(map[string]*TableFields)
	for name, t := range m.Tables {
		if t.Type == tableType || t.Type == "geometry" {
			result[name] = t.TableFields()
		}
	}
	return result
}

func (m *Mapping) extraTags(tableType TableType, tags map[Key]bool) {
	for _, t := range m.Tables {
		if t.Type != tableType {
			continue
		}
		for key, _ := range t.ExtraTags() {
			tags[key] = true
		}
		if t.Filters != nil && t.Filters.ExcludeTags != nil {
			for _, keyVal := range *t.Filters.ExcludeTags {
				tags[Key(keyVal[0])] = true
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
