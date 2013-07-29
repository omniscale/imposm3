package writer

import (
	"goposm/cache"
	"goposm/database"
	"goposm/element"
	"goposm/geom"
	"goposm/geom/geos"
	"goposm/mapping"
	"goposm/proj"
	"goposm/stats"
	"log"
	"sync"
)

type WayWriter struct {
	OsmElemWriter
	ways                 chan *element.Way
	lineStringTagMatcher *mapping.TagMatcher
	polygonTagMatcher    *mapping.TagMatcher
}

func NewWayWriter(osmCache *cache.OSMCache, diffCache *cache.DiffCache, ways chan *element.Way,
	insertBuffer database.RowInserter, lineStringTagMatcher *mapping.TagMatcher,
	polygonTagMatcher *mapping.TagMatcher, progress *stats.Statistics, srid int) *OsmElemWriter {
	ww := WayWriter{
		OsmElemWriter: OsmElemWriter{
			osmCache:     osmCache,
			diffCache:    diffCache,
			progress:     progress,
			wg:           &sync.WaitGroup{},
			insertBuffer: insertBuffer,
			srid:         srid,
		},
		ways:                 ways,
		lineStringTagMatcher: lineStringTagMatcher,
		polygonTagMatcher:    polygonTagMatcher,
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
		inserted, err := ww.osmCache.InsertedWays.IsInserted(w.Id)
		if err != nil {
			log.Println(err)
			continue
		}
		if inserted {
			continue
		}

		err = ww.osmCache.Coords.FillWay(w)
		if err != nil {
			continue
		}
		proj.NodesToMerc(w.Nodes)

		inserted = false
		if matches := ww.lineStringTagMatcher.Match(&w.Tags); len(matches) > 0 {
			ww.buildAndInsert(geos, w, matches, geom.LineString)
			inserted = true
		}
		if w.IsClosed() {
			if matches := ww.polygonTagMatcher.Match(&w.Tags); len(matches) > 0 {
				ww.buildAndInsert(geos, w, matches, geom.Polygon)
				inserted = true
			}
		}

		if inserted && ww.expireTiles != nil {
			ww.expireTiles.ExpireFromNodes(w.Nodes)
		}
		if ww.diffCache != nil {
			ww.diffCache.Coords.AddFromWay(w)
		}
	}
	ww.wg.Done()
}

type geomBuilder func(*geos.Geos, []element.Node) (*geos.Geom, error)

func (ww *WayWriter) buildAndInsert(geos *geos.Geos, w *element.Way, matches []mapping.Match, builder geomBuilder) {
	var err error
	// make copy to avoid interference with polygon/linestring matches
	way := element.Way(*w)
	geosgeom, err := builder(geos, way.Nodes)
	if err != nil {
		if err, ok := err.(ErrorLevel); ok {
			if err.Level() <= 0 {
				return
			}
		}
		log.Println(err)
		return
	}

	way.Geom, err = geom.AsGeomElement(geos, geosgeom)
	if err != nil {
		log.Println(err)
		return
	}

	if ww.clipper != nil {
		parts, err := ww.clipper.Clip(way.Geom.Geom)
		if err != nil {
			log.Println(err)
			return
		}
		for _, g := range parts {
			way := element.Way(*w)
			way.Geom = &element.Geometry{g, geos.AsEwkbHex(g)}
			ww.insertMatches(&way.OSMElem, matches)
		}
	} else {
		ww.insertMatches(&way.OSMElem, matches)
	}
}
