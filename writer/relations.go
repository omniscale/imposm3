package writer

import (
	"imposm3/cache"
	"imposm3/database"
	"imposm3/element"
	"imposm3/expire"
	"imposm3/geom"
	"imposm3/geom/geos"
	"imposm3/proj"
	"imposm3/stats"
	"sync"
	"time"
)

type RelationWriter struct {
	OsmElemWriter
	rel chan *element.Relation
}

func NewRelationWriter(osmCache *cache.OSMCache, diffCache *cache.DiffCache, rel chan *element.Relation,
	inserter database.Inserter, progress *stats.Statistics,
	srid int) *OsmElemWriter {
	rw := RelationWriter{
		OsmElemWriter: OsmElemWriter{
			osmCache:  osmCache,
			diffCache: diffCache,
			progress:  progress,
			wg:        &sync.WaitGroup{},
			inserter:  inserter,
			srid:      srid,
		},
		rel: rel,
	}
	rw.OsmElemWriter.writer = &rw
	return &rw.OsmElemWriter
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
			proj.NodesToMerc(m.Way.Nodes)
		}

		// BuildRelation updates r.Members but we need all of them
		// for the diffCache
		allMembers := r.Members

		// prepare relation first (build rings and compute actual
		// relation tags)
		prepedRel, err := geom.PrepareRelation(r, rw.srid)
		if err != nil {
			if err, ok := err.(ErrorLevel); ok {
				if err.Level() <= 0 {
					continue NextRel
				}
			}
			log.Warn(err)
			continue NextRel
		}

		// check for matches befor building the geometry
		ok, matches := rw.inserter.ProbePolygon(r.OSMElem)
		if !ok {
			continue NextRel
		}

		// build the multipolygon
		r, err = prepedRel.Build()
		if err != nil {
			if r.Geom != nil && r.Geom.Geom != nil {
				geos.Destroy(r.Geom.Geom)
			}
			if err, ok := err.(ErrorLevel); ok {
				if err.Level() <= 0 {
					continue NextRel

				}
			}
			log.Warn(err)
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
				rel.Id = -r.Id
				rel.Geom = &element.Geometry{Geom: g, Wkb: geos.AsEwkbHex(g)}
				err := rw.inserter.InsertPolygon(rel.OSMElem, matches)
				if err != nil {
					log.Warn(err)
					continue NextRel
				}
			}
		} else {
			rel := element.Relation(*r)
			rel.Id = -r.Id
			err := rw.inserter.InsertPolygon(rel.OSMElem, matches)
			if err != nil {
				log.Warn(err)
				continue NextRel
			}
		}

		for _, m := range rw.inserter.SelectRelationPolygons(r.Tags, r.Members) {
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
