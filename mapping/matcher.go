package mapping

import (
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/geom"
)

func (m *Mapping) PointMatcher() NodeMatcher {
	mappings := make(TagTableMapping)
	m.mappings(PointTable, mappings)
	filters := make(tableElementFilters)
	m.addFilters(filters)
	m.addTypedFilters(PointTable, filters)
	return &tagMatcher{
		mappings:   mappings,
		tables:     m.tables(PointTable),
		filters:    filters,
		matchAreas: false,
	}
}

func (m *Mapping) LineStringMatcher() WayMatcher {
	mappings := make(TagTableMapping)
	m.mappings(LineStringTable, mappings)
	filters := make(tableElementFilters)
	m.addFilters(filters)
	m.addTypedFilters(LineStringTable, filters)
	return &tagMatcher{
		mappings:   mappings,
		tables:     m.tables(LineStringTable),
		filters:    filters,
		matchAreas: false,
	}
}

func (m *Mapping) PolygonMatcher() RelWayMatcher {
	mappings := make(TagTableMapping)
	m.mappings(PolygonTable, mappings)
	filters := make(tableElementFilters)
	m.addFilters(filters)
	m.addTypedFilters(PolygonTable, filters)
	relFilters := make(tableElementFilters)
	m.addRelationFilters(PolygonTable, relFilters)
	return &tagMatcher{
		mappings:   mappings,
		tables:     m.tables(PolygonTable),
		filters:    filters,
		relFilters: relFilters,
		matchAreas: true,
	}
}

func (m *Mapping) RelationMatcher() RelationMatcher {
	mappings := make(TagTableMapping)
	m.mappings(RelationTable, mappings)
	filters := make(tableElementFilters)
	m.addFilters(filters)
	m.addTypedFilters(PolygonTable, filters)
	m.addTypedFilters(RelationTable, filters)
	relFilters := make(tableElementFilters)
	m.addRelationFilters(RelationTable, relFilters)
	return &tagMatcher{
		mappings:   mappings,
		tables:     m.tables(RelationTable),
		filters:    filters,
		relFilters: relFilters,
		matchAreas: true,
	}
}

func (m *Mapping) RelationMemberMatcher() RelationMatcher {
	mappings := make(TagTableMapping)
	m.mappings(RelationMemberTable, mappings)
	filters := make(tableElementFilters)
	m.addFilters(filters)
	m.addTypedFilters(RelationMemberTable, filters)
	relFilters := make(tableElementFilters)
	m.addRelationFilters(RelationMemberTable, relFilters)
	return &tagMatcher{
		mappings:   mappings,
		tables:     m.tables(RelationMemberTable),
		filters:    filters,
		relFilters: relFilters,
		matchAreas: true,
	}
}

type NodeMatcher interface {
	MatchNode(node *element.Node) []Match
}

type WayMatcher interface {
	MatchWay(way *element.Way) []Match
}

type RelationMatcher interface {
	MatchRelation(rel *element.Relation) []Match
}

type RelWayMatcher interface {
	WayMatcher
	RelationMatcher
}

type Match struct {
	Key     string
	Value   string
	Table   DestTable
	builder *rowBuilder
}

func (m *Match) Row(elem *element.OSMElem, geom *geom.Geometry) []interface{} {
	return m.builder.MakeRow(elem, geom, *m)
}

func (m *Match) MemberRow(rel *element.Relation, member *element.Member, geom *geom.Geometry) []interface{} {
	return m.builder.MakeMemberRow(rel, member, geom, *m)
}

type tagMatcher struct {
	mappings   TagTableMapping
	tables     map[string]*rowBuilder
	filters    tableElementFilters
	relFilters tableElementFilters
	matchAreas bool
}

func (tm *tagMatcher) MatchNode(node *element.Node) []Match {
	return tm.match(node.Tags, false, false)
}

func (tm *tagMatcher) MatchWay(way *element.Way) []Match {
	if tm.matchAreas { // match way as polygon
		if way.IsClosed() {
			if way.Tags["area"] == "no" {
				return nil
			}
			return tm.match(way.Tags, true, false)
		}
	} else { // match way as linestring
		if way.IsClosed() {
			if way.Tags["area"] == "yes" {
				return nil
			}
			return tm.match(way.Tags, true, false)
		}
		return tm.match(way.Tags, false, false)
	}
	return nil
}

func (tm *tagMatcher) MatchRelation(rel *element.Relation) []Match {
	return tm.match(rel.Tags, true, true)
}

type orderedMatch struct {
	Match
	order int
}

func (tm *tagMatcher) match(tags element.Tags, closed bool, relation bool) []Match {
	tables := make(map[DestTable]orderedMatch)

	addTables := func(k, v string, tbls []orderedDestTable) {
		for _, t := range tbls {
			this := orderedMatch{
				Match: Match{
					Key:     k,
					Value:   v,
					Table:   t.DestTable,
					builder: tm.tables[t.Name],
				},
				order: t.order,
			}
			if other, ok := tables[t.DestTable]; ok {
				if other.order < this.order {
					this = other
				}
			}
			tables[t.DestTable] = this
		}
	}

	if values, ok := tm.mappings[Key("__any__")]; ok {
		addTables("__any__", "__any__", values["__any__"])
	}

	for k, v := range tags {
		values, ok := tm.mappings[Key(k)]
		if ok {
			if tbls, ok := values["__any__"]; ok {
				addTables(k, v, tbls)
			}
			if tbls, ok := values[Value(v)]; ok {
				addTables(k, v, tbls)
			}
		}
	}
	var matches []Match
	for t, match := range tables {
		filters, ok := tm.filters[t.Name]
		filteredOut := false
		if ok {
			for _, filter := range filters {
				if !filter(tags, Key(match.Key), closed) {
					filteredOut = true
					break
				}
			}
		}
		if relation && !filteredOut {
			filters, ok := tm.relFilters[t.Name]
			if ok {
				for _, filter := range filters {
					if !filter(tags, Key(match.Key), closed) {
						filteredOut = true
						break
					}
				}
			}
		}

		if !filteredOut {
			matches = append(matches, match.Match)
		}
	}
	return matches
}

type valueBuilder struct {
	key     Key
	colType ColumnType
}

func (v *valueBuilder) Value(elem *element.OSMElem, geom *geom.Geometry, match Match) interface{} {
	if v.colType.Func != nil {
		return v.colType.Func(elem.Tags[string(v.key)], elem, geom, match)
	}
	return nil
}

func (v *valueBuilder) MemberValue(rel *element.Relation, member *element.Member, geom *geom.Geometry, match Match) interface{} {
	if v.colType.Func != nil {
		if v.colType.FromMember {
			if member.Elem == nil {
				return nil
			}
			return v.colType.Func(member.Elem.Tags[string(v.key)], member.Elem, geom, match)
		}
		return v.colType.Func(rel.Tags[string(v.key)], &rel.OSMElem, geom, match)
	}
	if v.colType.MemberFunc != nil {
		return v.colType.MemberFunc(rel, member, match)
	}
	return nil
}

type rowBuilder struct {
	columns []valueBuilder
}

func (r *rowBuilder) MakeRow(elem *element.OSMElem, geom *geom.Geometry, match Match) []interface{} {
	var row []interface{}
	for _, column := range r.columns {
		row = append(row, column.Value(elem, geom, match))
	}
	return row
}

func (r *rowBuilder) MakeMemberRow(rel *element.Relation, member *element.Member, geom *geom.Geometry, match Match) []interface{} {
	var row []interface{}
	for _, column := range r.columns {
		row = append(row, column.MemberValue(rel, member, geom, match))
	}
	return row
}

// SelectRelationPolygons returns a slice of all members that are already
// imported as part of the relation.
// Outer members are "imported" if they share the same destination table. Inner members
// are "imported" when they also share the same key/value.
func SelectRelationPolygons(polygonTagMatcher RelWayMatcher, rel *element.Relation) []element.Member {
	relMatches := polygonTagMatcher.MatchRelation(rel)
	result := []element.Member{}
	for _, m := range rel.Members {
		if m.Type != element.WAY {
			continue
		}
		memberMatches := polygonTagMatcher.MatchWay(m.Way)
		if m.Role == "outer" && dstEquals(relMatches, memberMatches) {
			result = append(result, m)
		} else if matchEquals(relMatches, memberMatches) {
			result = append(result, m)
		}
	}
	return result
}

// matchEquals returns true if both matches share key/value and table
func matchEquals(matchesA, matchesB []Match) bool {
	for _, matchA := range matchesA {
		for _, matchB := range matchesB {
			if matchA.Key == matchB.Key &&
				matchA.Value == matchB.Value &&
				matchA.Table == matchB.Table {
				return true
			}
		}
	}
	return false
}

// dstEquals returns true if both matches share a single destination table
func dstEquals(matchesA, matchesB []Match) bool {
	for _, matchA := range matchesA {
		for _, matchB := range matchesB {
			if matchA.Table == matchB.Table {
				return true
			}
		}
	}
	return false
}
