package writer

import (
	"fmt"
	"goposm/cache"
	"goposm/element"
	"goposm/geom"
	"goposm/mapping"
	"goposm/proj"
	"goposm/stats"
	"log"
	"runtime"
	"sync"
)

type RelationWriter struct {
	osmCache     *cache.OSMCache
	rel          chan *element.Relation
	tagMatcher   *mapping.TagMatcher
	progress     *stats.Statistics
	insertBuffer *InsertBuffer
	wg           *sync.WaitGroup
}

func NewRelationWriter(osmCache *cache.OSMCache, rel chan *element.Relation,
	insertBuffer *InsertBuffer, tagMatcher *mapping.TagMatcher, progress *stats.Statistics) *RelationWriter {
	rw := RelationWriter{
		osmCache:     osmCache,
		rel:          rel,
		insertBuffer: insertBuffer,
		tagMatcher:   tagMatcher,
		progress:     progress,
		wg:           &sync.WaitGroup{},
	}

	for i := 0; i < runtime.NumCPU(); i++ {
		rw.wg.Add(1)
		go rw.loop()
	}
	return &rw
}

func (rw *RelationWriter) Close() {
	rw.wg.Wait()
}

func (rw *RelationWriter) loop() {
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
			for _, match := range matches {
				row := match.Row(&r.OSMElem)
				rw.insertBuffer.Insert(match.Table, row)
			}
			err := rw.osmCache.InsertedWays.PutMembers(r.Members)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
	rw.wg.Done()
}
