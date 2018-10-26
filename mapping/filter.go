package mapping

import (
	"path"
	"strings"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/mapping/config"
)

type TagFilterer interface {
	Filter(tags *osm.Tags)
}

func (m *Mapping) NodeTagFilter() TagFilterer {
	if m.Conf.Tags.LoadAll {
		return newExcludeFilter(m.Conf.Tags.Exclude)
	}
	mappings := make(TagTableMapping)
	m.mappings(PointTable, mappings)
	tags := make(map[Key]bool)
	m.extraTags(PointTable, tags)
	m.extraTags(RelationMemberTable, tags)
	return &tagFilter{mappings.asTagMap(), tags}
}

func (m *Mapping) WayTagFilter() TagFilterer {
	if m.Conf.Tags.LoadAll {
		return newExcludeFilter(m.Conf.Tags.Exclude)
	}
	mappings := make(TagTableMapping)
	m.mappings(LineStringTable, mappings)
	m.mappings(PolygonTable, mappings)
	tags := make(map[Key]bool)
	m.extraTags(LineStringTable, tags)
	m.extraTags(PolygonTable, tags)
	m.extraTags(RelationMemberTable, tags)
	return &tagFilter{mappings.asTagMap(), tags}
}

func (m *Mapping) RelationTagFilter() TagFilterer {
	if m.Conf.Tags.LoadAll {
		return newExcludeFilter(m.Conf.Tags.Exclude)
	}
	mappings := make(TagTableMapping)
	// do not filter out type tag for common relations
	mappings["type"] = map[Value][]orderedDestTable{
		"multipolygon": []orderedDestTable{},
		"boundary":     []orderedDestTable{},
		"land_area":    []orderedDestTable{},
	}
	m.mappings(LineStringTable, mappings)
	m.mappings(PolygonTable, mappings)
	m.mappings(RelationTable, mappings)
	m.mappings(RelationMemberTable, mappings)
	tags := make(map[Key]bool)
	m.extraTags(LineStringTable, tags)
	m.extraTags(PolygonTable, tags)
	m.extraTags(RelationTable, tags)
	m.extraTags(RelationMemberTable, tags)
	return &tagFilter{mappings.asTagMap(), tags}
}

type tagMap map[Key]map[Value]struct{}

type tagFilter struct {
	mappings  tagMap
	extraTags map[Key]bool
}

func (f *tagFilter) Filter(tags *osm.Tags) {
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

type excludeFilter struct {
	keys    map[Key]struct{}
	matches []string
}

func newExcludeFilter(tags []config.Key) *excludeFilter {
	f := excludeFilter{
		keys:    make(map[Key]struct{}),
		matches: make([]string, 0),
	}
	for _, t := range tags {
		if strings.ContainsAny(string(t), "?*[") {
			f.matches = append(f.matches, string(t))
		} else {
			f.keys[Key(t)] = struct{}{}
		}
	}
	return &f
}

func (f *excludeFilter) Filter(tags *osm.Tags) {
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
