package mapping

import (
	"io/ioutil"
	"regexp"

	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/log"
	"github.com/omniscale/imposm3/mapping/config"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

type orderedDestTable struct {
	DestTable
	order int
}

type TagTableMapping map[Key]map[Value][]orderedDestTable

func (tt TagTableMapping) addFromMapping(mapping config.KeyValues, table DestTable) {
	for key, vals := range mapping {
		for _, v := range vals {
			vals, ok := tt[Key(key)]
			tbl := orderedDestTable{DestTable: table, order: v.Order}
			if ok {
				vals[Value(v.Value)] = append(vals[Value(v.Value)], tbl)
			} else {
				tt[Key(key)] = make(map[Value][]orderedDestTable)
				tt[Key(key)][Value(v.Value)] = append(tt[Key(key)][Value(v.Value)], tbl)
			}
		}
	}
}

func (tt TagTableMapping) asTagMap() tagMap {
	result := make(tagMap)
	for k, vals := range tt {
		result[k] = make(map[Value]struct{})
		for v := range vals {
			result[k][v] = struct{}{}
		}
	}
	return result
}

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
	case `"relation"`:
		*tt = RelationTable
	case `"relation_member"`:
		*tt = RelationMemberTable
	}
	return errors.New("unknown type " + string(data))
}

const (
	PolygonTable        TableType = "polygon"
	LineStringTable     TableType = "linestring"
	PointTable          TableType = "point"
	GeometryTable       TableType = "geometry"
	RelationTable       TableType = "relation"
	RelationMemberTable TableType = "relation_member"
)

type Mapping struct {
	Conf                  config.Mapping
	PointMatcher          NodeMatcher
	LineStringMatcher     WayMatcher
	PolygonMatcher        RelWayMatcher
	RelationMatcher       RelationMatcher
	RelationMemberMatcher RelationMatcher
}

func FromFile(filename string) (*Mapping, error) {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return New(b)
}

func New(b []byte) (*Mapping, error) {
	mapping := Mapping{}
	err := yaml.Unmarshal(b, &mapping.Conf)
	if err != nil {
		return nil, err
	}

	err = mapping.prepare()
	if err != nil {
		return nil, err
	}

	err = mapping.createMatcher()
	if err != nil {
		return nil, err
	}
	return &mapping, nil
}

func (m *Mapping) prepare() error {
	for name, t := range m.Conf.Tables {
		t.Name = name
		if t.OldFields != nil {
			// todo deprecate 'fields'
			t.Columns = t.OldFields
		}
		if t.Type == "" {
			return errors.Errorf("missing type for table %s", name)
		}

		if TableType(t.Type) == GeometryTable {
			if t.Mapping != nil || t.Mappings != nil {
				return errors.Errorf("table with type:geometry requires type_mapping for table %s", name)
			}
		}
	}

	for name, t := range m.Conf.GeneralizedTables {
		t.Name = name
	}
	return nil
}

func (m *Mapping) createMatcher() error {
	var err error
	m.PointMatcher, err = m.pointMatcher()
	if err != nil {
		return err
	}
	m.LineStringMatcher, err = m.lineStringMatcher()
	if err != nil {
		return err
	}
	m.PolygonMatcher, err = m.polygonMatcher()
	if err != nil {
		return err
	}
	m.RelationMatcher, err = m.relationMatcher()
	if err != nil {
		return err
	}
	m.RelationMemberMatcher, err = m.relationMemberMatcher()
	if err != nil {
		return err
	}
	return nil
}

func (m *Mapping) mappings(tableType TableType, mappings TagTableMapping) {
	for name, t := range m.Conf.Tables {
		if TableType(t.Type) != GeometryTable && TableType(t.Type) != tableType {
			continue
		}
		mappings.addFromMapping(t.Mapping, DestTable{Name: name})

		for subMappingName, subMapping := range t.Mappings {
			mappings.addFromMapping(subMapping.Mapping, DestTable{Name: name, SubMapping: subMappingName})
		}

		switch tableType {
		case PointTable:
			mappings.addFromMapping(t.TypeMappings.Points, DestTable{Name: name})
		case LineStringTable:
			mappings.addFromMapping(t.TypeMappings.LineStrings, DestTable{Name: name})
		case PolygonTable:
			mappings.addFromMapping(t.TypeMappings.Polygons, DestTable{Name: name})
		}
	}
}

func (m *Mapping) tables(tableType TableType) (map[string]*rowBuilder, error) {
	var err error
	result := make(map[string]*rowBuilder)
	for name, t := range m.Conf.Tables {
		if TableType(t.Type) == tableType || TableType(t.Type) == GeometryTable {
			result[name], err = makeRowBuilder(t)
			if err != nil {
				return nil, errors.Wrapf(err, "creating row builder for %s", name)
			}

		}
	}
	return result, nil
}

func makeRowBuilder(tbl *config.Table) (*rowBuilder, error) {
	result := rowBuilder{}

	for _, mappingColumn := range tbl.Columns {
		column := valueBuilder{}
		column.key = Key(mappingColumn.Key)

		columnType, err := MakeColumnType(mappingColumn)
		if err != nil {
			return nil, errors.Wrapf(err, "creating column %s", mappingColumn.Name)
		}
		column.colType = *columnType
		result.columns = append(result.columns, column)
	}
	return &result, nil
}

func MakeColumnType(c *config.Column) (*ColumnType, error) {
	columnType, ok := AvailableColumnTypes[c.Type]
	if !ok {
		return nil, errors.Errorf("unhandled type %s", c.Type)
	}

	if columnType.MakeFunc != nil {
		makeValue, err := columnType.MakeFunc(c.Name, columnType, *c)
		if err != nil {
			return nil, err
		}
		columnType = ColumnType{columnType.Name, columnType.GoType, makeValue, nil, nil, columnType.FromMember}
	}
	columnType.FromMember = c.FromMember
	return &columnType, nil
}

func (m *Mapping) extraTags(tableType TableType, tags map[Key]bool) {
	for _, t := range m.Conf.Tables {
		if TableType(t.Type) != tableType && TableType(t.Type) != GeometryTable {
			continue
		}

		for _, col := range t.Columns {
			if col.Key != "" {
				tags[Key(col.Key)] = true
			}
			for _, k := range col.Keys {
				tags[Key(k)] = true
			}
		}

		if t.Filters != nil && t.Filters.ExcludeTags != nil {
			for _, keyVal := range *t.Filters.ExcludeTags {
				tags[Key(keyVal[0])] = true
			}
		}

		if tableType == PolygonTable || tableType == RelationTable || tableType == RelationMemberTable {
			if t.RelationTypes != nil {
				tags["type"] = true
			}
		}
	}
	for _, k := range m.Conf.Tags.Include {
		tags[Key(k)] = true
	}

	// always include area tag for closed-way handling
	tags["area"] = true
}

type elementFilter func(tags element.Tags, key Key, closed bool) bool

type tableElementFilters map[string][]elementFilter

func (m *Mapping) addTypedFilters(tableType TableType, filters tableElementFilters) {
	var areaTags map[Key]struct{}
	var linearTags map[Key]struct{}
	if m.Conf.Areas.AreaTags != nil {
		areaTags = make(map[Key]struct{})
		for _, tag := range m.Conf.Areas.AreaTags {
			areaTags[Key(tag)] = struct{}{}
		}
	}
	if m.Conf.Areas.LinearTags != nil {
		linearTags = make(map[Key]struct{})
		for _, tag := range m.Conf.Areas.LinearTags {
			linearTags[Key(tag)] = struct{}{}
		}
	}

	for name, t := range m.Conf.Tables {
		if TableType(t.Type) != GeometryTable && TableType(t.Type) != tableType {
			continue
		}
		if TableType(t.Type) == LineStringTable && areaTags != nil {
			f := func(tags element.Tags, key Key, closed bool) bool {
				if closed {
					if tags["area"] == "yes" {
						return false
					}
					if tags["area"] != "no" {
						if _, ok := areaTags[key]; ok {
							return false
						}
					}
				}
				return true
			}
			filters[name] = append(filters[name], f)
		}
		if TableType(t.Type) == PolygonTable && linearTags != nil {
			f := func(tags element.Tags, key Key, closed bool) bool {
				if closed && tags["area"] == "no" {
					return false
				}
				if tags["area"] != "yes" {
					if _, ok := linearTags[key]; ok {
						return false
					}
				}
				return true
			}
			filters[name] = append(filters[name], f)
		}
	}
}

func (m *Mapping) addRelationFilters(tableType TableType, filters tableElementFilters) {
	for name, t := range m.Conf.Tables {
		if t.RelationTypes != nil {
			relTypes := t.RelationTypes // copy loop var for closure
			f := func(tags element.Tags, key Key, closed bool) bool {
				if v, ok := tags["type"]; ok {
					for _, rtype := range relTypes {
						if v == rtype {
							return true
						}
					}
				}
				return false
			}
			filters[name] = append(filters[name], f)
		} else {
			if TableType(t.Type) == PolygonTable {
				// standard multipolygon handling (boundary and land_area are for backwards compatibility)
				f := func(tags element.Tags, key Key, closed bool) bool {
					if v, ok := tags["type"]; ok {
						if v == "multipolygon" || v == "boundary" || v == "land_area" {
							return true
						}
					}
					return false
				}
				filters[name] = append(filters[name], f)
			}
		}
	}
}

func (m *Mapping) addFilters(filters tableElementFilters) {
	for name, t := range m.Conf.Tables {
		if t.Filters == nil {
			continue
		}
		if t.Filters.ExcludeTags != nil {
			log.Println("[warn]: exclude_tags filter is deprecated and will be removed. See require and reject filter.")
			for _, filterKeyVal := range *t.Filters.ExcludeTags {
				// Convert `exclude_tags`` filter to `reject` filter !
				keyname := filterKeyVal[0]
				vararr := []config.OrderedValue{
					{
						Value: config.Value(filterKeyVal[1]),
						Order: 1,
					},
				}
				filters[name] = append(filters[name], makeFiltersFunction(name, false, true, keyname, vararr))

			}
		}

		if t.Filters.Require != nil {
			for keyname, vararr := range t.Filters.Require {
				filters[name] = append(filters[name], makeFiltersFunction(name, true, false, string(keyname), vararr))
			}
		}

		if t.Filters.Reject != nil {
			for keyname, vararr := range t.Filters.Reject {
				filters[name] = append(filters[name], makeFiltersFunction(name, false, true, string(keyname), vararr))
			}
		}

		if t.Filters.RequireRegexp != nil {
			for keyname, regexp := range t.Filters.RequireRegexp {
				filters[name] = append(filters[name], makeRegexpFiltersFunction(name, true, false, string(keyname), regexp))
			}
		}

		if t.Filters.RejectRegexp != nil {
			for keyname, regexp := range t.Filters.RejectRegexp {
				filters[name] = append(filters[name], makeRegexpFiltersFunction(name, false, true, string(keyname), regexp))
			}
		}

	}
}

func findValueInOrderedValue(v config.Value, list []config.OrderedValue) bool {
	for _, item := range list {
		if item.Value == v {
			return true
		}
	}
	return false
}

func makeRegexpFiltersFunction(tablename string, virtualTrue bool, virtualFalse bool, vKeyname string, vRegexp string) func(tags element.Tags, key Key, closed bool) bool {
	// Compile regular expression,  if not valid regexp --> panic !
	r := regexp.MustCompile(vRegexp)
	return func(tags element.Tags, key Key, closed bool) bool {
		if v, ok := tags[vKeyname]; ok {
			if r.MatchString(v) {
				return virtualTrue
			}
		}
		return virtualFalse
	}
}

func makeFiltersFunction(tablename string, virtualTrue bool, virtualFalse bool, vKeyname string, vVararr []config.OrderedValue) func(tags element.Tags, key Key, closed bool) bool {

	if findValueInOrderedValue("__nil__", vVararr) { // check __nil__
		log.Println("[warn] Filter value '__nil__' is not supported ! (tablename:" + tablename + ")")
	}

	if findValueInOrderedValue("__any__", vVararr) { // check __any__
		if len(vVararr) > 1 {
			log.Println("[warn] Multiple filter value with '__any__' keywords is not valid! (tablename:" + tablename + ")")
		}
		return func(tags element.Tags, key Key, closed bool) bool {
			if _, ok := tags[vKeyname]; ok {
				return virtualTrue
			}
			return virtualFalse
		}
	} else if len(vVararr) == 1 { //  IF 1 parameter  THEN we can generate optimal code
		return func(tags element.Tags, key Key, closed bool) bool {
			if v, ok := tags[vKeyname]; ok {
				if config.Value(v) == vVararr[0].Value {
					return virtualTrue
				}
			}
			return virtualFalse
		}
	} else { //  > 1 parameter  - less optimal code
		return func(tags element.Tags, key Key, closed bool) bool {
			if v, ok := tags[vKeyname]; ok {
				if findValueInOrderedValue(config.Value(v), vVararr) {
					return virtualTrue
				}
			}
			return virtualFalse
		}
	}
}
