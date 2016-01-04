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
			continue NextRel
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

		relMemberMatches := rw.relationMemberMatcher.MatchRelation(r)
		if len(relMemberMatches) > 0 {
			for i, m := range r.Members {
				if m.Type == element.RELATION {
					mrel, err := rw.osmCache.Relations.GetRelation(m.Id)
					if err != nil {
						if err == cache.NotFound {
							log.Warn(err)
							continue NextRel
						}
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
								continue NextRel
							}
						} else {
							log.Warn(err)
							continue NextRel
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
					continue
				}

				var gelem geomp.Geometry
				if g == nil {
					g = geos.FromWkt("POLYGON EMPTY")
					gelem = geomp.Geometry{Geom: g, Wkb: geos.AsEwkbHex(g)}
				} else {
					gelem, err = geomp.AsGeomElement(geos, g)
					if err != nil {
						log.Warn(err)
						continue
					}
				}

				rw.inserter.InsertRelationMember(*r, m, gelem, relMemberMatches)
			}
		}

		relMatches := rw.relationMatcher.MatchRelation(r)
		if len(relMatches) > 0 {
			rw.inserter.InsertPolygon(r.OSMElem, geomp.Geometry{}, relMatches)
		}

		// BuildRelation updates r.Members but we need all of them
		// for the diffCache
		allMembers := r.Members

		// prepare relation first (build rings and compute actual
		// relation tags)
		prepedRel, err := geomp.PrepareRelation(r, rw.srid, rw.maxGap)
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
