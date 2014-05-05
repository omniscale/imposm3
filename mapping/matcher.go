package mapping

import (
	"imposm3/element"
)

func (m *Mapping) PointMatcher() *TagMatcher {
	mappings := make(TagTables)
	m.mappings("point", mappings)
	filters := m.ElementFilters()
	return &TagMatcher{mappings, m.tables("point"), filters}
}

func (m *Mapping) LineStringMatcher() *TagMatcher {
	mappings := make(TagTables)
	m.mappings("linestring", mappings)
	filters := m.ElementFilters()
	return &TagMatcher{mappings, m.tables("linestring"), filters}
}

func (m *Mapping) PolygonMatcher() *TagMatcher {
	mappings := make(TagTables)
	m.mappings("polygon", mappings)
	filters := m.ElementFilters()
	return &TagMatcher{mappings, m.tables("polygon"), filters}
}

type TagMatcher struct {
	mappings TagTables
	tables   map[string]*TableFields
	filters  map[string][]ElementFilter
}

type Match struct {
	Key         string
	Value       string
	Table       DestTable
	tableFields *TableFields
}

func (m *Match) Row(elem *element.OSMElem) []interface{} {
	return m.tableFields.MakeRow(elem, *m)
}

func (tagMatcher *TagMatcher) Match(tags *element.Tags) []Match {
	tables := make(map[DestTable]Match)

	for k, v := range *tags {
		values, ok := tagMatcher.mappings[Key(k)]
		if ok {
			if tbls, ok := values["__any__"]; ok {
				for _, t := range tbls {
					tables[t] = Match{k, v, t, tagMatcher.tables[t.Name]}
				}
			}
			if tbls, ok := values[Value(v)]; ok {
				for _, t := range tbls {
					tables[t] = Match{k, v, t, tagMatcher.tables[t.Name]}
				}
			}
		}
	}
	var matches []Match
	for t, match := range tables {
		filters, ok := tagMatcher.filters[t.Name]
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
func SelectRelationPolygons(polygonTagMatcher *TagMatcher, tags element.Tags, members []element.Member) []element.Member {
	relMatches := polygonTagMatcher.Match(&tags)
	result := []element.Member{}
	for _, m := range members {
		if m.Type != element.WAY {
			continue
		}
		memberMatches := polygonTagMatcher.Match(&m.Way.Tags)
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
