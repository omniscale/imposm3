package writer

import (
	"sync"
	"time"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/database"
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/expire"
	geomp "github.com/omniscale/imposm3/geom"
	geosp "github.com/omniscale/imposm3/geom/geos"
	"github.com/omniscale/imposm3/log"
	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/stats"
)

type RelationWriter struct {
	OsmElemWriter
	singleIDSpace         bool
	rel                   chan *osm.Relation
	polygonMatcher        mapping.RelWayMatcher
	relationMatcher       mapping.RelationMatcher
	relationMemberMatcher mapping.RelationMatcher
	maxGap                float64
}

func NewRelationWriter(
	osmCache *cache.OSMCache,
	diffCache *cache.DiffCache,
	singleIDSpace bool,
	rel chan *osm.Relation,
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
		singleIDSpace:         singleIDSpace,
		polygonMatcher:        matcher,
		relationMatcher:       relMatcher,
		relationMemberMatcher: relMemberMatcher,
		rel:    rel,
		maxGap: maxGap,
	}
	rw.OsmElemWriter.writer = &rw
	return &rw.OsmElemWriter
}

func (rw *RelationWriter) relID(id int64) int64 {
	if !rw.singleIDSpace {
		return -id
	}
	return element.RelIDOffset - id
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
				log.Println("[warn]: ", err)
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
					log.Println("[warn]: ", err)
				}
				continue NextRel
			}
			rw.NodesToSrid(m.Way.Nodes)
			r.Members[i].Element = &m.Way.Element
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
			rw.diffCache.Ways.AddFromMembers(r.ID, allMembers)
			rw.diffCache.CoordsRel.AddFromMembers(r.ID, allMembers)
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

func handleMultiPolygon(rw *RelationWriter, r *osm.Relation, geos *geosp.Geos) bool {
	matches := rw.polygonMatcher.MatchRelation(r)
	if matches == nil {
		return false
	}

	// prepare relation (build rings)
	prepedRel, err := geomp.PrepareRelation(r, rw.srid, rw.maxGap)
	if err != nil {
		if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
			log.Println("[warn]: ", err)
		}
		return false
	}

	// build the multipolygon
	geom, err := prepedRel.Build()
	if geom.Geom != nil {
		defer geos.Destroy(geom.Geom)
	}
	if err != nil {
		if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
			log.Println("[warn]: ", err)
		}
		return false
	}

	if rw.limiter != nil {
		start := time.Now()
		parts, err := rw.limiter.Clip(geom.Geom)
		if err != nil {
			log.Println("[warn]: ", err)
			return false
		}
		if duration := time.Now().Sub(start); duration > time.Minute {
			log.Printf("[warn]: clipping relation %d to -limitto took %s", r.ID, duration)
		}
		if len(parts) == 0 {
			return false
		}
		for _, g := range parts {
			rel := osm.Relation(*r)
			rel.ID = rw.relID(r.ID)
			geom = geomp.Geometry{Geom: g, Wkb: geos.AsEwkbHex(g)}
			err := rw.inserter.InsertPolygon(rel.Element, geom, matches)
			if err != nil {
				if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
					log.Println("[warn]: ", err)
				}
				continue
			}
		}
	} else {
		rel := osm.Relation(*r)
		rel.ID = rw.relID(r.ID)
		err := rw.inserter.InsertPolygon(rel.Element, geom, matches)
		if err != nil {
			if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
				log.Println("[warn]: ", err)
			}
			return false
		}
	}

	return true
}

func handleRelation(rw *RelationWriter, r *osm.Relation, geos *geosp.Geos) bool {
	relMatches := rw.relationMatcher.MatchRelation(r)
	if relMatches == nil {
		return false
	}
	rel := osm.Relation(*r)
	rel.ID = rw.relID(r.ID)
	rw.inserter.InsertPolygon(rel.Element, geomp.Geometry{}, relMatches)
	return true
}

func handleRelationMembers(rw *RelationWriter, r *osm.Relation, geos *geosp.Geos) bool {
	relMemberMatches := rw.relationMemberMatcher.MatchRelation(r)
	if relMemberMatches == nil {
		return false
	}
	for i, m := range r.Members {
		if m.Type == osm.RelationMember {
			mrel, err := rw.osmCache.Relations.GetRelation(m.ID)
			if err != nil {
				if err != cache.NotFound {
					log.Println("[warn]: ", err)
				}
				return false
			}
			r.Members[i].Element = &mrel.Element
		} else if m.Type == osm.NodeMember {
			nd, err := rw.osmCache.Nodes.GetNode(m.ID)
			if err != nil {
				if err == cache.NotFound {
					nd, err = rw.osmCache.Coords.GetCoord(m.ID)
					if err != nil {
						if err != cache.NotFound {
							log.Println("[warn]: ", err)
						}
						return false
					}
				} else {
					log.Println("[warn]: ", err)
					return false
				}
			}
			rw.NodeToSrid(nd)
			r.Members[i].Node = nd
			r.Members[i].Element = &nd.Element
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
			log.Println("[warn]: ", err)
			return false
		}

		var gelem geomp.Geometry
		if g == nil {
			g = geos.FromWkt("POLYGON EMPTY")
			gelem = geomp.Geometry{Geom: g, Wkb: geos.AsEwkbHex(g)}
		} else {
			gelem, err = geomp.AsGeomElement(geos, g)
			if err != nil {
				log.Println("[warn]: ", err)
				return false
			}
		}
		rel := osm.Relation(*r)
		rel.ID = rw.relID(r.ID)
		rw.inserter.InsertRelationMember(rel, m, gelem, relMemberMatches)
	}
	return true
}
