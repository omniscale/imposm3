package writer

import (
	"sync"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/database"
	"github.com/omniscale/imposm3/expire"
	geomp "github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/geom/geos"
	"github.com/omniscale/imposm3/log"
	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/stats"
)

type WayWriter struct {
	OsmElemWriter
	singleIDSpace  bool
	ways           chan *osm.Way
	lineMatcher    mapping.WayMatcher
	polygonMatcher mapping.WayMatcher
	maxGap         float64
}

func NewWayWriter(
	osmCache *cache.OSMCache,
	diffCache *cache.DiffCache,
	singleIDSpace bool,
	ways chan *osm.Way,
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
		singleIDSpace:  singleIDSpace,
		lineMatcher:    lineMatcher,
		polygonMatcher: polygonMatcher,
		ways:           ways,
		maxGap:         maxGap,
	}
	ww.OsmElemWriter.writer = &ww
	return &ww.OsmElemWriter
}

func (ww *WayWriter) wayID(id int64) int64 {
	if !ww.singleIDSpace {
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

		filled := false
		// fill loads all coords. call only if we have a match
		fill := func(w *osm.Way) bool {
			if filled {
				return true
			}
			err := ww.osmCache.Coords.FillWay(w)
			if err != nil {
				return false
			}
			ww.NodesToSrid(w.Nodes)
			filled = true
			return true
		}

		var err error
		var status insertStatus
		inserted := false
		insertedPolygon := false
		if matches := ww.lineMatcher.MatchWay(w); len(matches) > 0 {
			if !fill(w) {
				continue
			}
			status, err = ww.buildAndInsert(geos, w, matches, false)
			inserted = status == insertSucceeded
			if err != nil {
				if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
					log.Println("[warn]: ", err)
				}
				if status == insertFailed && ww.diffCache != nil {
					// cache anyway, so that an updated node is able to fix the way
					ww.diffCache.Coords.AddFromWay(w)
				}
				continue
			}
		}
		if matches := ww.polygonMatcher.MatchWay(w); len(matches) > 0 {
			if !fill(w) {
				continue
			}
			if w.IsClosed() {
				status, err = ww.buildAndInsert(geos, w, matches, true)
				insertedPolygon = status == insertSucceeded
				if err != nil {
					if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
						log.Println("[warn]: ", err)
					}
					if status == insertFailed && ww.diffCache != nil {
						// cache anyway, so that an updated node is able to fix the way
						ww.diffCache.Coords.AddFromWay(w)
					}
					continue
				}
			}
		}

		if (inserted || insertedPolygon) && ww.expireor != nil {
			expire.ExpireProjectedNodes(ww.expireor, w.Nodes, ww.srid, insertedPolygon)
		}
		if (inserted || insertedPolygon) && ww.diffCache != nil {
			ww.diffCache.Coords.AddFromWay(w)
		}
	}
	ww.wg.Done()
}

func (ww *WayWriter) buildAndInsert(
	g *geos.Geos,
	w *osm.Way,
	matches []mapping.Match,
	isPolygon bool,
) (insertStatus, error) {

	// make copy to avoid interference with polygon/linestring matches
	way := osm.Way(*w)
	way.ID = ww.wayID(way.ID)

	var err error
	var geosgeom *geos.Geom

	if isPolygon {
		geosgeom, err = geomp.Polygon(g, way.Nodes)
		if err == nil {
			geosgeom, err = g.MakeValid(geosgeom)
		}
	} else {
		geosgeom, err = geomp.LineString(g, way.Nodes)
	}
	if err != nil {
		return insertFailed, err
	}

	geom, err := geomp.AsGeomElement(g, geosgeom)
	if err != nil {
		return insertFailed, err
	}

	status := insertSucceeded
	if ww.limiter != nil {
		parts, err := ww.limiter.Clip(geom.Geom)
		if err != nil {
			return insertFailed, err
		}
		if len(parts) == 0 {
			// outside of limitto
			status = insertSkipped
		}
		for _, p := range parts {
			geom = geomp.Geometry{Geom: p, Wkb: g.AsEwkbHex(p)}
			if isPolygon {
				if err := ww.inserter.InsertPolygon(way.Element, geom, matches); err != nil {
					return insertFailed, err
				}
			} else {
				if err := ww.inserter.InsertLineString(way.Element, geom, matches); err != nil {
					return insertFailed, err
				}
			}
		}
	} else {
		if isPolygon {
			if err := ww.inserter.InsertPolygon(way.Element, geom, matches); err != nil {
				return insertFailed, err
			}
		} else {
			if err := ww.inserter.InsertLineString(way.Element, geom, matches); err != nil {
				return insertFailed, err
			}
		}
	}
	return status, nil
}
