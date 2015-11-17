package writer

import (
	"sync"
	"time"

	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/database"
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/expire"
	geomp "github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/geom/geos"
	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/stats"
)

type RelationWriter struct {
	OsmElemWriter
	singleIdSpace  bool
	rel            chan *element.Relation
	polygonMatcher mapping.RelWayMatcher
	lineMatcher mapping.RelWayMatcher
	maxGap         float64
}

func NewRelationWriter(
	osmCache *cache.OSMCache,
	diffCache *cache.DiffCache,
	singleIdSpace bool,
	rel chan *element.Relation,
	inserter database.Inserter,
	progress *stats.Statistics,
	polygonMatcher mapping.RelWayMatcher,
	lineMatcher mapping.RelWayMatcher,
	srid int,
) *OsmElemWriter {
	maxGap := 1e-1 // 0.1m
	if srid == 4326 {
		maxGap = 1e-6 // ~0.1m
	}
	rw := RelationWriter{
		OsmElemWriter: OsmElemWriter{
			osmCache:  osmCache,
			diffCache: diffCache,
			progress:  progress,
			wg:        &sync.WaitGroup{},
			inserter:  inserter,
			srid:      srid,
		},
		singleIdSpace:  singleIdSpace,
		polygonMatcher: polygonMatcher,
		lineMatcher:	lineMatcher,
		rel:            rel,
		maxGap:         maxGap,
	}
	rw.OsmElemWriter.writer = &rw
	return &rw.OsmElemWriter
}

func (rw *RelationWriter) relId(id int64) int64 {
	if !rw.singleIdSpace {
		return -id
	}
	return element.RelIdOffset - id
}

func (rw *RelationWriter) loop() {
	geos := geos.NewGeos()
	geos.SetHandleSrid(rw.srid)
	defer geos.Finish()

NextRel:
	for r := range rw.rel {
		rw.progress.AddRelations(1)
		err := rw.osmCache.Ways.FillMembers(r.Members)
		if err != nil {
			if err != cache.NotFound {
				log.Warn(err)
			}
			continue NextRel
		}
		for _, m := range r.Members {
			if m.Way == nil {
				continue
			}
			err := rw.osmCache.Coords.FillWay(m.Way)
			if err != nil {
				if err != cache.NotFound {
					log.Warn(err)
				}
				continue NextRel
			}
			rw.NodesToSrid(m.Way.Nodes)
		}

		// BuildRelation updates r.Members but we need all of them
		// for the diffCache
		allMembers := r.Members

		//if relation type is route then proccess relation ways
		if r.Tags["type"] == "route" {
			if matches := rw.lineMatcher.MatchRelation(r); len(matches) > 0 {
				for _, member := range allMembers {
					if member.Way == nil {
						continue
					}
					//switch way tags to relation tags and insert way
					originalTags := member.Way.Tags
					member.Way.Tags = r.Tags
					err := rw.buildAndInsertWay(geos, member.Way, matches, false)
					if err != nil {
						if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
							log.Warn(err)
						}
						continue NextRel
					}
					//return back original way Tags
					member.Way.Tags = originalTags
				}
			}
		}

		// prepare relation first (build rings and compute actual
		// relation tags)
		prepedRel, err := geomp.PrepareRelation(r, rw.srid, rw.maxGap)
		if err != nil {
			if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
				log.Warn(err)
			}
			continue NextRel
		}

		// check for matches before building the geometry
		matches := rw.polygonMatcher.MatchRelation(r)
		if len(matches) == 0 {
			continue NextRel
		}

		// build the multipolygon
		geom, err := prepedRel.Build()
		if err != nil {
			if geom.Geom != nil {
				geos.Destroy(geom.Geom)
			}
			if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
				log.Warn(err)
			}
			continue NextRel
		}

		if rw.limiter != nil {
			start := time.Now()
			parts, err := rw.limiter.Clip(geom.Geom)
			if err != nil {
				log.Warn(err)
				continue NextRel
			}
			if duration := time.Now().Sub(start); duration > time.Minute {
				log.Warnf("clipping relation %d to -limitto took %s", r.Id, duration)
			}
			for _, g := range parts {
				rel := element.Relation(*r)
				rel.Id = rw.relId(r.Id)
				geom = geomp.Geometry{Geom: g, Wkb: geos.AsEwkbHex(g)}
				err := rw.inserter.InsertPolygon(rel.OSMElem, geom, matches)
				if err != nil {
					if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
						log.Warn(err)
					}
					continue
				}
			}
		} else {
			rel := element.Relation(*r)
			rel.Id = rw.relId(r.Id)
			err := rw.inserter.InsertPolygon(rel.OSMElem, geom, matches)
			if err != nil {
				if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
					log.Warn(err)
				}
				continue
			}
		}

		for _, m := range mapping.SelectRelationPolygons(rw.polygonMatcher, r) {
			err = rw.osmCache.InsertedWays.PutWay(m.Way)
			if err != nil {
				log.Warn(err)
			}
		}
		if rw.diffCache != nil {
			rw.diffCache.Ways.AddFromMembers(r.Id, allMembers)
			for _, member := range allMembers {
				if member.Way != nil {
					rw.diffCache.Coords.AddFromWay(member.Way)
				}
			}
		}
		if rw.expireor != nil {
			for _, m := range allMembers {
				if m.Way != nil {
					expire.ExpireNodes(rw.expireor, m.Way.Nodes)
				}
			}
		}
		geos.Destroy(geom.Geom)
	}
	rw.wg.Done()
}