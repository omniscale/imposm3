package writer

import (
	"fmt"
	"goposm/cache"
	"goposm/element"
	"goposm/geom"
	"goposm/geom/geos"
	"goposm/mapping"
	"goposm/proj"
	"goposm/stats"
	"log"
	"sync"
)

type RelationWriter struct {
	OsmElemWriter
	rel        chan *element.Relation
	tagMatcher *mapping.TagMatcher
}

func NewRelationWriter(osmCache *cache.OSMCache, rel chan *element.Relation,
	insertBuffer *InsertBuffer, tagMatcher *mapping.TagMatcher, progress *stats.Statistics) *OsmElemWriter {
	rw := RelationWriter{
		OsmElemWriter: OsmElemWriter{
			osmCache:     osmCache,
			progress:     progress,
			wg:           &sync.WaitGroup{},
			insertBuffer: insertBuffer,
		},
		rel:        rel,
		tagMatcher: tagMatcher,
	}
	rw.OsmElemWriter.writer = &rw
	return &rw.OsmElemWriter
}

func (rw *RelationWriter) loop() {
	geos := geos.NewGeos()
	defer geos.Finish()

	for r := range rw.rel {
		rw.progress.AddRelations(1)
		err := rw.osmCache.Ways.FillMembers(r.Members)

		if err == cache.NotFound {
			// fmt.Println("missing ways for relation", r.Id)
		} else if err != nil {
			fmt.Println(err)
			continue
		}
		for _, m := range r.Members {
			if m.Way == nil {
				continue
			}
			err := rw.osmCache.Coords.FillWay(m.Way)
			if err == cache.NotFound {
				// fmt.Println("missing nodes for way", m.Way.Id, "in relation", r.Id)
			} else if err != nil {
				fmt.Println(err)
				continue
			}
			proj.NodesToMerc(m.Way.Nodes)
		}

		err = geom.BuildRelation(r)
		if err != nil {
			if err, ok := err.(ErrorLevel); ok {
				if err.Level() <= 0 {
					continue
				}
			}
			log.Println(err)
			continue
		}
		if matches := rw.tagMatcher.Match(&r.Tags); len(matches) > 0 {
			if rw.clipper != nil {
				parts, err := rw.clipper.Clip(r.Geom.Geom)
				if err != nil {
					log.Println(err)
					continue
				}
				for _, g := range parts {
					rel := element.Relation(*r)
					rel.Geom = &element.Geometry{g, geos.AsWkb(g)}
					rw.insertMatches(&r.OSMElem, matches)
				}
			} else {
				rw.insertMatches(&r.OSMElem, matches)
			}
			err := rw.osmCache.InsertedWays.PutMembers(r.Members)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
	rw.wg.Done()
}
