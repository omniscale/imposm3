package mapping

import (
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/geom"
)

func (m *Mapping) PointMatcher() NodeMatcher {
	mappings := make(TagTables)
	m.mappings("point", mappings)
	filters := m.ElementFilters()
	return &tagMatcher{mappings, m.tables("point"), filters, false}
}

func (m *Mapping) LineStringMatcher() RelWayMatcher {
	mappings := make(TagTables)
	m.mappings("linestring", mappings)
	filters := m.ElementFilters()
	return &tagMatcher{mappings, m.tables("linestring"), filters, false}
}

func (m *Mapping) PolygonMatcher() RelWayMatcher {
	mappings := make(TagTables)
	m.mappings("polygon", mappings)
	filters := m.ElementFilters()
	return &tagMatcher{mappings, m.tables("polygon"), filters, true}
}

type Match struct {
	Key         string
	Value       string
	Table       DestTable
	tableFields *TableFields
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

type tagMatcher struct {
	mappings   TagTables
	tables     map[string]*TableFields
	filters    map[string][]ElementFilter
	matchAreas bool
}

func (m *Match) Row(elem *element.OSMElem, geom *geom.Geometry) []interface{} {
	return m.tableFields.MakeRow(elem, geom, *m)
}

func (tm *tagMatcher) MatchNode(node *element.Node) []Match {
	return tm.match(&node.Tags)
}

func (tm *tagMatcher) MatchWay(way *element.Way) []Match {
	if tm.matchAreas { // match way as polygon
		if way.IsClosed() {
			if way.Tags["area"] == "no" {
				return nil
			}
			return tm.match(&way.Tags)
		}
	} else { // match way as linestring
		if way.IsClosed() {
			if way.Tags["area"] == "yes" {
				return nil
			}
		}
		return tm.match(&way.Tags)
	}
	return nil
}

func (tm *tagMatcher) MatchRelation(rel *element.Relation) []Match {
	return tm.match(&rel.Tags)
}

type orderedMatch struct {
	Match
	order int
}

func (tm *tagMatcher) match(tags *element.Tags) []Match {
	tables := make(map[DestTable]orderedMatch)

	addTables := func(k, v string, tbls []OrderedDestTable) {
		for _, t := range tbls {
			this := orderedMatch{
				Match: Match{k, v, t.DestTable, tm.tables[t.Name]},
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

	for k, v := range *tags {
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
				if !filter(tags) {
					filteredOut = true
					break
				}
			}
		}
		if !filteredOut {
			matches = append(matches, match.Match)
		}
	}
	return matches
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
