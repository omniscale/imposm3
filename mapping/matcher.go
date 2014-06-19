package mapping

import (
	"imposm3/element"
)

func (m *Mapping) PointMatcher() NodeMatcher {
	mappings := make(TagTables)
	m.mappings("point", mappings)
	filters := m.ElementFilters()
	return &tagMatcher{mappings, m.tables("point"), filters, false}
}

func (m *Mapping) LineStringMatcher() WayMatcher {
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

func (m *Match) Row(elem *element.OSMElem) []interface{} {
	return m.tableFields.MakeRow(elem, *m)
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

func (tm *tagMatcher) match(tags *element.Tags) []Match {
	tables := make(map[DestTable]Match)

	for k, v := range *tags {
		values, ok := tm.mappings[Key(k)]
		if ok {
			if tbls, ok := values["__any__"]; ok {
				for _, t := range tbls {
					tables[t] = Match{k, v, t, tm.tables[t.Name]}
				}
			}
			if tbls, ok := values[Value(v)]; ok {
				for _, t := range tbls {
					tables[t] = Match{k, v, t, tm.tables[t.Name]}
				}
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
			matches = append(matches, match)
		}
	}
	return matches
}

// SelectRelationPolygons returns a slice of all members that are already
// imported with a relation with tags.
func SelectRelationPolygons(polygonTagMatcher RelWayMatcher, rel *element.Relation) []element.Member {
	relMatches := polygonTagMatcher.MatchRelation(rel)
	result := []element.Member{}
	for _, m := range rel.Members {
		if m.Type != element.WAY {
			continue
		}
		memberMatches := polygonTagMatcher.MatchWay(m.Way)
		if matchEquals(relMatches, memberMatches) {
			result = append(result, m)
		}
	}
	return result
}

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
