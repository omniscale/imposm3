package mapping

import (
	"path"
	"strings"

	"github.com/omniscale/imposm3/element"
)

func (m *Mapping) NodeTagFilter() TagFilterer {
	if m.Tags.LoadAll {
		return newExcludeFilter(m.Tags.Exclude)
	}
	mappings := make(map[Key]map[Value][]OrderedDestTable)
	m.mappings("point", mappings)
	tags := make(map[Key]bool)
	m.extraTags("point", tags)
	return &TagFilter{mappings, tags}
}

func (m *Mapping) WayTagFilter() TagFilterer {
	if m.Tags.LoadAll {
		return newExcludeFilter(m.Tags.Exclude)
	}
	mappings := make(map[Key]map[Value][]OrderedDestTable)
	m.mappings("linestring", mappings)
	m.mappings("polygon", mappings)
	tags := make(map[Key]bool)
	m.extraTags("linestring", tags)
	m.extraTags("polygon", tags)
	return &TagFilter{mappings, tags}
}

func (m *Mapping) RelationTagFilter() TagFilterer {
	if m.Tags.LoadAll {
		return newExcludeFilter(m.Tags.Exclude)
	}
	mappings := make(map[Key]map[Value][]OrderedDestTable)
	// do not filter out type tag for common relations
	mappings["type"] = map[Value][]OrderedDestTable{
		"multipolygon": []OrderedDestTable{},
		"boundary":     []OrderedDestTable{},
		"land_area":    []OrderedDestTable{},
	}
	m.mappings("linestring", mappings)
	m.mappings("polygon", mappings)
	tags := make(map[Key]bool)
	m.extraTags("linestring", tags)
	m.extraTags("polygon", tags)
	return &TagFilter{mappings, tags}
}

type TagFilter struct {
	mappings  map[Key]map[Value][]OrderedDestTable
	extraTags map[Key]bool
}

type ExcludeFilter struct {
	keys    map[Key]struct{}
	matches []string
}

func newExcludeFilter(tags []Key) *ExcludeFilter {
	f := ExcludeFilter{
		keys:    make(map[Key]struct{}),
		matches: make([]string, 0),
	}
	for _, t := range tags {
		if strings.ContainsAny(string(t), "?*[") {
			f.matches = append(f.matches, string(t))
		} else {
			f.keys[t] = struct{}{}
		}
	}
	return &f
}

func (f *ExcludeFilter) Filter(tags *element.Tags) {
	for k := range *tags {
		if _, ok := f.keys[Key(k)]; ok {
			delete(*tags, k)
		} else if f.matches != nil {
			for _, exkey := range f.matches {
				if ok, _ := path.Match(exkey, k); ok {
					delete(*tags, k)
					break
				}
			}
		}
	}
}

type TagFilterer interface {
	Filter(tags *element.Tags)
}

func (f *TagFilter) Filter(tags *element.Tags) {
	if tags == nil {
		return
	}
	for k, v := range *tags {
		values, ok := f.mappings[Key(k)]
		if ok {
			if _, ok := values["__any__"]; ok {
				continue
			} else if _, ok := values[Value(v)]; ok {
				continue
			} else if _, ok := f.extraTags[Key(k)]; !ok {
				delete(*tags, k)
			}
		} else if _, ok := f.extraTags[Key(k)]; !ok {
			delete(*tags, k)
		}
	}
}
