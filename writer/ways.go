package writer

import (
	"imposm3/cache"
	"imposm3/database"
	"imposm3/element"
	"imposm3/expire"
	"imposm3/geom"
	"imposm3/geom/geos"
	"imposm3/mapping"
	"imposm3/proj"
	"imposm3/stats"
	"sync"
)

type WayWriter struct {
	OsmElemWriter
	ways           chan *element.Way
	lineMatcher    mapping.WayMatcher
	polygonMatcher mapping.WayMatcher
}

func NewWayWriter(
	osmCache *cache.OSMCache,
	diffCache *cache.DiffCache,
	ways chan *element.Way,
	inserter database.Inserter,
	progress *stats.Statistics,
	polygonMatcher mapping.WayMatcher,
	lineMatcher mapping.WayMatcher,
	srid int,
) *OsmElemWriter {
	ww := WayWriter{
		OsmElemWriter: OsmElemWriter{
			osmCache:  osmCache,
			diffCache: diffCache,
			progress:  progress,
			wg:        &sync.WaitGroup{},
			inserter:  inserter,
			srid:      srid,
		},
		lineMatcher:    lineMatcher,
		polygonMatcher: polygonMatcher,
		ways:           ways,
	}
	ww.OsmElemWriter.writer = &ww
	return &ww.OsmElemWriter
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
		proj.NodesToMerc(w.Nodes)

		inserted := false
		if matches := ww.lineMatcher.MatchWay(w); len(matches) > 0 {
			err := ww.buildAndInsert(geos, w, matches, false)
			if err != nil {
				if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
					log.Warn(err)
				}
				continue
			}
			inserted = true
		}
		if w.IsClosed() && !insertedAsRelation {
			// only add polygons that were not inserted as a MultiPolygon relation
			if matches := ww.polygonMatcher.MatchWay(w); len(matches) > 0 {
				err := ww.buildAndInsert(geos, w, matches, true)
				if err != nil {
					if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
						log.Warn(err)
					}
					continue
				}
				inserted = true
			}
		}

		if inserted && ww.expireor != nil {
			expire.ExpireNodes(ww.expireor, w.Nodes)
		}
		if ww.diffCache != nil {
			ww.diffCache.Coords.AddFromWay(w)
		}
	}
	ww.wg.Done()
}

func (ww *WayWriter) buildAndInsert(g *geos.Geos, w *element.Way, matches []mapping.Match, isPolygon bool) error {
	var err error
	var geosgeom *geos.Geom
	// make copy to avoid interference with polygon/linestring matches
	way := element.Way(*w)

	if isPolygon {
		geosgeom, err = geom.Polygon(g, way.Nodes)
	} else {
		geosgeom, err = geom.LineString(g, way.Nodes)
	}
	if err != nil {
		return err
	}

	way.Geom, err = geom.AsGeomElement(g, geosgeom)
	if err != nil {
		return err
	}

	if ww.limiter != nil {
		parts, err := ww.limiter.Clip(way.Geom.Geom)
		if err != nil {
			return err
		}
		for _, p := range parts {
			way := element.Way(*w)
			way.Geom = &element.Geometry{Geom: p, Wkb: g.AsEwkbHex(p)}
			if isPolygon {
				if err := ww.inserter.InsertPolygon(way.OSMElem, matches); err != nil {
					return err
				}
			} else {
				if err := ww.inserter.InsertLineString(way.OSMElem, matches); err != nil {
					return err
				}
			}
		}
	} else {
		if isPolygon {
			if err := ww.inserter.InsertPolygon(way.OSMElem, matches); err != nil {
				return err
			}
		} else {
			if err := ww.inserter.InsertLineString(way.OSMElem, matches); err != nil {
				return err
			}
		}
	}
	return nil
}
