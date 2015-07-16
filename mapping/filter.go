package mapping

import (
	"github.com/omniscale/imposm3/element"
        "path"
)

func (m *Mapping) NodeTagFilter() TagFilterer {
	if m.Tags.LoadAll {
		return newExcludeFilter(m.Tags.Exclude)
	}
	mappings := make(map[Key]map[Value][]DestTable)
	m.mappings("point", mappings)
	tags := make(map[Key]bool)
	m.extraTags("point", tags)
	return &TagFilter{mappings, tags}
}

func (m *Mapping) WayTagFilter() TagFilterer {
	if m.Tags.LoadAll {
		return newExcludeFilter(m.Tags.Exclude)
	}
	mappings := make(map[Key]map[Value][]DestTable)
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
	mappings := make(map[Key]map[Value][]DestTable)
	m.mappings("linestring", mappings)
	m.mappings("polygon", mappings)
	tags := make(map[Key]bool)
	m.extraTags("linestring", tags)
	m.extraTags("polygon", tags)
	// do not filter out type tag
	mappings["type"] = map[Value][]DestTable{
		"multipolygon": []DestTable{},
		"boundary":     []DestTable{},
		"land_area":    []DestTable{},
	}
	return &RelationTagFilter{TagFilter{mappings, tags}}
}

type TagFilter struct {
	mappings  map[Key]map[Value][]DestTable
	extraTags map[Key]bool
}

type RelationTagFilter struct {
	TagFilter
}

type ExcludeFilter struct {
	exclude map[Key]struct{}
}

func newExcludeFilter(tags []Key) *ExcludeFilter {
	f := ExcludeFilter{make(map[Key]struct{}, len(tags))}
	for _, tag := range tags {
		f.exclude[tag] = struct{}{}
	}
	return &f
}

func (f *ExcludeFilter) Filter(tags *element.Tags) bool {
	for k, _ := range *tags {
		for exkey, _ := range f.exclude {
			if ok, _ := path.Match(string(exkey),k); ok {
				delete(*tags, k)
				break;
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
