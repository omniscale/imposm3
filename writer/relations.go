package writer

import (
	"sync"
	"time"

	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/database"
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/expire"
	"github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/geom/geos"
	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/stats"
)

type RelationWriter struct {
	OsmElemWriter
	singleIdSpace  bool
	rel            chan *element.Relation
	polygonMatcher mapping.RelWayMatcher
	maxGap         float64
}

func NewRelationWriter(
	osmCache *cache.OSMCache,
	diffCache *cache.DiffCache,
	singleIdSpace bool,
	rel chan *element.Relation,
	inserter database.Inserter,
	progress *stats.Statistics,
	matcher mapping.RelWayMatcher,
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
		polygonMatcher: matcher,
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

		// prepare relation first (build rings and compute actual
		// relation tags)
		prepedRel, err := geom.PrepareRelation(r, rw.srid, rw.maxGap)
		if err != nil {
			if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
				log.Warn(err)
			}
			continue NextRel
		}

		// check for matches befor building the geometry
		matches := rw.polygonMatcher.MatchRelation(r)
		if len(matches) == 0 {
			continue NextRel
		}

		// build the multipolygon
		r, err = prepedRel.Build()
		if err != nil {
			if r.Geom != nil && r.Geom.Geom != nil {
				geos.Destroy(r.Geom.Geom)
			}
			if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
				log.Warn(err)
			}
			continue NextRel
		}

		if rw.limiter != nil {
			start := time.Now()
			parts, err := rw.limiter.Clip(r.Geom.Geom)
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
				rel.Geom = &element.Geometry{Geom: g, Wkb: geos.AsEwkbHex(g)}
				err := rw.inserter.InsertPolygon(rel.OSMElem, matches)
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
			err := rw.inserter.InsertPolygon(rel.OSMElem, matches)
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
		geos.Destroy(r.Geom.Geom)
	}
	rw.wg.Done()
}
