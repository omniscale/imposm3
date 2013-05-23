package mapping

import (
	"goposm/element"
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
	Table       string
	tableFields *TableFields
}

func (m *Match) Row(elem *element.OSMElem) []interface{} {
	return m.tableFields.MakeRow(elem, *m)
}

func (tagMatcher *TagMatcher) Match(tags *element.Tags) []Match {
	tables := make(map[string]Match)

	for k, v := range *tags {
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
