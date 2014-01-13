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
)

type WayWriter struct {
	OsmElemWriter
	ways chan *element.Way
}

func NewWayWriter(osmCache *cache.OSMCache, diffCache *cache.DiffCache, ways chan *element.Way,
	inserter database.Inserter,
	progress *stats.Statistics, srid int) *OsmElemWriter {
	ww := WayWriter{
		OsmElemWriter: OsmElemWriter{
			osmCache:  osmCache,
			diffCache: diffCache,
			progress:  progress,
			wg:        &sync.WaitGroup{},
			inserter:  inserter,
			srid:      srid,
		},
		ways: ways,
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
		if ok, matches := ww.inserter.ProbeLineString(w.OSMElem); ok {
			ww.buildAndInsert(geos, w, matches, false)
			inserted = true
		}
		if w.IsClosed() && !insertedAsRelation {
			// only add polygons that were not inserted as a MultiPolygon relation
			if ok, matches := ww.inserter.ProbePolygon(w.OSMElem); ok {
				ww.buildAndInsert(geos, w, matches, true)
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

func (ww *WayWriter) buildAndInsert(g *geos.Geos, w *element.Way, matches interface{}, isPolygon bool) {
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
		if err, ok := err.(ErrorLevel); ok {
			if err.Level() <= 0 {
				return
			}
		}
		log.Warn(err)
		return
	}

	way.Geom, err = geom.AsGeomElement(g, geosgeom)
	if err != nil {
		log.Warn(err)
		return
	}

	if ww.limiter != nil {
		parts, err := ww.limiter.Clip(way.Geom.Geom)
		if err != nil {
			log.Warn(err)
			return
		}
		for _, p := range parts {
			way := element.Way(*w)
			way.Geom = &element.Geometry{Geom: p, Wkb: g.AsEwkbHex(p)}
			if isPolygon {
				ww.inserter.InsertPolygon(way.OSMElem, matches)
			} else {
				ww.inserter.InsertLineString(way.OSMElem, matches)
			}
		}
	} else {
		if isPolygon {
			ww.inserter.InsertPolygon(way.OSMElem, matches)
		} else {
			ww.inserter.InsertLineString(way.OSMElem, matches)
		}
	}
}
