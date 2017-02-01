package writer

import (
	"sync"

	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/database"
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/expire"
	geomp "github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/geom/geos"
	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/stats"
)

type WayWriter struct {
	OsmElemWriter
	singleIdSpace  bool
	ways           chan *element.Way
	lineMatcher    mapping.WayMatcher
	polygonMatcher mapping.WayMatcher
	maxGap         float64
}

func NewWayWriter(
	osmCache *cache.OSMCache,
	diffCache *cache.DiffCache,
	singleIdSpace bool,
	ways chan *element.Way,
	inserter database.Inserter,
	progress *stats.Statistics,
	polygonMatcher mapping.WayMatcher,
	lineMatcher mapping.WayMatcher,
	srid int,
) *OsmElemWriter {
	maxGap := 1e-1 // 0.1m
	if srid == 4326 {
		maxGap = 1e-6 // ~0.1m
	}
	ww := WayWriter{
		OsmElemWriter: OsmElemWriter{
			osmCache:  osmCache,
			diffCache: diffCache,
			progress:  progress,
			wg:        &sync.WaitGroup{},
			inserter:  inserter,
			srid:      srid,
		},
		singleIdSpace:  singleIdSpace,
		lineMatcher:    lineMatcher,
		polygonMatcher: polygonMatcher,
		ways:           ways,
		maxGap:         maxGap,
	}
	ww.OsmElemWriter.writer = &ww
	return &ww.OsmElemWriter
}

func (ww *WayWriter) wayId(id int64) int64 {
	if !ww.singleIdSpace {
		return id
	}
	return -id
}

func (ww *WayWriter) loop() {
	geos := geos.NewGeos()
	geos.SetHandleSrid(ww.srid)
	defer geos.Finish()
	for w := range ww.ways {
		ww.progress.AddWays(1)
		if len(w.Tags) == 0 {
			continue
		}
		insertedAsRelation, err := ww.osmCache.InsertedWays.IsInserted(w.Id)
		if err != nil {
			log.Warn(err)
			continue
		}

		err = ww.osmCache.Coords.FillWay(w)
		if err != nil {
			continue
		}
		ww.NodesToSrid(w.Nodes)

		w.Id = ww.wayId(w.Id)

		inserted := false
		insertedPolygon := false
		if matches := ww.lineMatcher.MatchWay(w); len(matches) > 0 {
			err, inserted = ww.buildAndInsert(geos, w, matches, false)
			if err != nil {
				if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
					log.Warn(err)
				}
				continue
			}
		}
		if !insertedAsRelation && (w.IsClosed() || w.TryClose(ww.maxGap)) {
			// only add polygons that were not inserted as a MultiPolygon relation
			if matches := ww.polygonMatcher.MatchWay(w); len(matches) > 0 {
				err, insertedPolygon = ww.buildAndInsert(geos, w, matches, true)
				if err != nil {
					if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
						log.Warn(err)
					}
					continue
				}
			}
		}

		if (inserted || insertedPolygon) && ww.expireor != nil {
			expire.ExpireProjectedNodes(ww.expireor, w.Nodes, ww.srid, insertedPolygon)
		}
		if ww.diffCache != nil {
			ww.diffCache.Coords.AddFromWay(w)
		}
	}
	ww.wg.Done()
}

func (ww *WayWriter) buildAndInsert(g *geos.Geos, w *element.Way, matches []mapping.Match, isPolygon bool) (error, bool) {
	var err error
	var geosgeom *geos.Geom
	// make copy to avoid interference with polygon/linestring matches
	way := element.Way(*w)

	if isPolygon {
		geosgeom, err = geomp.Polygon(g, way.Nodes)
		if err == nil {
			geosgeom, err = g.MakeValid(geosgeom)
		}
	} else {
		geosgeom, err = geomp.LineString(g, way.Nodes)
	}
	if err != nil {
		return err, false
	}

	geom, err := geomp.AsGeomElement(g, geosgeom)
	if err != nil {
		return err, false
	}

	inserted := true
	if ww.limiter != nil {
		parts, err := ww.limiter.Clip(geom.Geom)
		if err != nil {
			return err, false
		}
		if len(parts) == 0 {
			// outside of limitto
			inserted = false
		}
		for _, p := range parts {
			way := element.Way(*w)
			geom = geomp.Geometry{Geom: p, Wkb: g.AsEwkbHex(p)}
			if isPolygon {
				if err := ww.inserter.InsertPolygon(way.OSMElem, geom, matches); err != nil {
					return err, false
				}
			} else {
				if err := ww.inserter.InsertLineString(way.OSMElem, geom, matches); err != nil {
					return err, false
				}
			}
		}
	} else {
		if isPolygon {
			if err := ww.inserter.InsertPolygon(way.OSMElem, geom, matches); err != nil {
				return err, false
			}
		} else {
			if err := ww.inserter.InsertLineString(way.OSMElem, geom, matches); err != nil {
				return err, false
			}
		}
	}
	return nil, inserted
}
