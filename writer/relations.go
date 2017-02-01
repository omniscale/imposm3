package writer

import (
	"sync"
	"time"

	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/database"
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/expire"
	geomp "github.com/omniscale/imposm3/geom"
	geosp "github.com/omniscale/imposm3/geom/geos"
	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/stats"
)

type RelationWriter struct {
	OsmElemWriter
	singleIdSpace         bool
	rel                   chan *element.Relation
	polygonMatcher        mapping.RelWayMatcher
	relationMatcher       mapping.RelationMatcher
	relationMemberMatcher mapping.RelationMatcher
	maxGap                float64
}

func NewRelationWriter(
	osmCache *cache.OSMCache,
	diffCache *cache.DiffCache,
	singleIdSpace bool,
	rel chan *element.Relation,
	inserter database.Inserter,
	progress *stats.Statistics,
	matcher mapping.RelWayMatcher,
	relMatcher mapping.RelationMatcher,
	relMemberMatcher mapping.RelationMatcher,
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
		singleIdSpace:         singleIdSpace,
		polygonMatcher:        matcher,
		relationMatcher:       relMatcher,
		relationMemberMatcher: relMemberMatcher,
		rel:    rel,
		maxGap: maxGap,
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
	geos := geosp.NewGeos()
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
			continue
		}
		for i, m := range r.Members {
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
			r.Members[i].Elem = &m.Way.OSMElem
		}

		// handleRelation updates r.Members but we need all of them
		// for the diffCache
		allMembers := r.Members

		inserted := false

		if handleRelationMembers(rw, r, geos) {
			inserted = true
		}
		if handleRelation(rw, r, geos) {
			inserted = true
		}
		if handleMultiPolygon(rw, r, geos) {
			inserted = true
		}

		if inserted && rw.diffCache != nil {
			rw.diffCache.Ways.AddFromMembers(r.Id, allMembers)
			rw.diffCache.CoordsRel.AddFromMembers(r.Id, allMembers)
			for _, member := range allMembers {
				if member.Way != nil {
					rw.diffCache.Coords.AddFromWay(member.Way)
				}
			}
		}
		if inserted && rw.expireor != nil {
			for _, m := range allMembers {
				if m.Way != nil {
					expire.ExpireProjectedNodes(rw.expireor, m.Way.Nodes, rw.srid, true)
				}
			}
		}
	}
	rw.wg.Done()
}

func handleMultiPolygon(rw *RelationWriter, r *element.Relation, geos *geosp.Geos) bool {
	// prepare relation first (build rings and compute actual
	// relation tags)
	prepedRel, err := geomp.PrepareRelation(r, rw.srid, rw.maxGap)
	if err != nil {
		if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
			log.Warn(err)
		}
		return false
	}

	// check for matches befor building the geometry
	matches := rw.polygonMatcher.MatchRelation(r)
	if matches == nil {
		return false
	}

	// build the multipolygon
	geom, err := prepedRel.Build()
	if geom.Geom != nil {
		defer geos.Destroy(geom.Geom)
	}
	if err != nil {
		if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
			log.Warn(err)
		}
		return false
	}

	if rw.limiter != nil {
		start := time.Now()
		parts, err := rw.limiter.Clip(geom.Geom)
		if err != nil {
			log.Warn(err)
			return false
		}
		if duration := time.Now().Sub(start); duration > time.Minute {
			log.Warnf("clipping relation %d to -limitto took %s", r.Id, duration)
		}
		if len(parts) == 0 {
			return false
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
			return false
		}
	}

	for _, m := range mapping.SelectRelationPolygons(rw.polygonMatcher, r) {
		err = rw.osmCache.InsertedWays.PutWay(m.Way)
		if err != nil {
			log.Warn(err)
		}
	}
	return true
}

func handleRelation(rw *RelationWriter, r *element.Relation, geos *geosp.Geos) bool {
	relMatches := rw.relationMatcher.MatchRelation(r)
	if relMatches == nil {
		return false
	}
	rel := element.Relation(*r)
	rel.Id = rw.relId(r.Id)
	rw.inserter.InsertPolygon(rel.OSMElem, geomp.Geometry{}, relMatches)
	return true
}

func handleRelationMembers(rw *RelationWriter, r *element.Relation, geos *geosp.Geos) bool {
	relMemberMatches := rw.relationMemberMatcher.MatchRelation(r)
	if relMemberMatches == nil {
		return false
	}
	for i, m := range r.Members {
		if m.Type == element.RELATION {
			mrel, err := rw.osmCache.Relations.GetRelation(m.Id)
			if err != nil {
				if err != cache.NotFound {
					log.Warn(err)
				}
				return false
			}
			r.Members[i].Elem = &mrel.OSMElem
		} else if m.Type == element.NODE {
			nd, err := rw.osmCache.Nodes.GetNode(m.Id)
			if err != nil {
				if err == cache.NotFound {
					nd, err = rw.osmCache.Coords.GetCoord(m.Id)
					if err != nil {
						if err != cache.NotFound {
							log.Warn(err)
						}
						return false
					}
				} else {
					log.Warn(err)
					return false
				}
			}
			rw.NodeToSrid(nd)
			r.Members[i].Node = nd
			r.Members[i].Elem = &nd.OSMElem
		}
	}

	for _, m := range r.Members {
		var g *geosp.Geom
		var err error
		if m.Node != nil {
			g, err = geomp.Point(geos, *m.Node)
		} else if m.Way != nil {
			g, err = geomp.LineString(geos, m.Way.Nodes)
		}

		if err != nil {
			log.Warn(err)
			return false
		}

		var gelem geomp.Geometry
		if g == nil {
			g = geos.FromWkt("POLYGON EMPTY")
			gelem = geomp.Geometry{Geom: g, Wkb: geos.AsEwkbHex(g)}
		} else {
			gelem, err = geomp.AsGeomElement(geos, g)
			if err != nil {
				log.Warn(err)
				return false
			}
		}
		rel := element.Relation(*r)
		rel.Id = rw.relId(r.Id)
		rw.inserter.InsertRelationMember(rel, m, gelem, relMemberMatches)
	}
	return true
}
