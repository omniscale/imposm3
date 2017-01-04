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
	m.mappings("linestring", mappings)
	m.mappings("polygon", mappings)
	tags := make(map[Key]bool)
	m.extraTags("linestring", tags)
	m.extraTags("polygon", tags)
	// do not filter out type tag
	mappings["type"] = map[Value][]OrderedDestTable{
		"multipolygon": []OrderedDestTable{},
		"boundary":     []OrderedDestTable{},
		"land_area":    []OrderedDestTable{},
	}
	return &RelationTagFilter{TagFilter{mappings, tags}}
}

type TagFilter struct {
	mappings  map[Key]map[Value][]OrderedDestTable
	extraTags map[Key]bool
}

type RelationTagFilter struct {
	TagFilter
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

func (f *ExcludeFilter) Filter(tags *element.Tags) bool {
	for k, _ := range *tags {
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
	return true
}

type TagFilterer interface {
	Filter(tags *element.Tags) bool
}

func (f *TagFilter) Filter(tags *element.Tags) bool {
	if tags == nil {
		return false
	}
	foundMapping := false
	for k, v := range *tags {
		values, ok := f.mappings[Key(k)]
		if ok {
			if _, ok := values["__any__"]; ok {
				foundMapping = true
				continue
			} else if _, ok := values[Value(v)]; ok {
				foundMapping = true
				continue
			} else if _, ok := f.extraTags[Key(k)]; !ok {
				delete(*tags, k)
			}
		} else if _, ok := f.extraTags[Key(k)]; !ok {
			delete(*tags, k)
		}
	}
	if foundMapping {
		return true
	} else {
		*tags = nil
		return false
	}
}

func (f *RelationTagFilter) Filter(tags *element.Tags) bool {
	if tags == nil {
		return false
	}

	// TODO improve filtering for relation/relation_member mappings
	// right now this only works with tags.load_all:true
	if t, ok := (*tags)["type"]; ok {
		if t != "multipolygon" && t != "boundary" && t != "land_area" {
			*tags = nil
			return false
		}
		if t == "boundary" {
			if _, ok := (*tags)["boundary"]; !ok {
				// a lot of the boundary relations are not multipolygon
				// only import with boundary tags (e.g. boundary=administrative)
				*tags = nil
				return false
			}
		}
	} else {
		*tags = nil
		return false
	}
	tagCount := len(*tags)
	f.TagFilter.Filter(tags)

	// we removed tags...
	if len(*tags) < tagCount {
		expectedTags := 0
		if _, ok := (*tags)["name"]; ok {
			expectedTags += 1
		}
		if _, ok := (*tags)["type"]; ok {
			expectedTags += 1
		}
		if len(*tags) == expectedTags {
			// but no tags except name and type are left
			// remove all, otherwise tags from longest
			// way/ring would be used during MP building
			*tags = nil
			return false
		}
	}
	// always return true here since we found a matching type
	return true
}
