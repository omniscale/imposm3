package writer

import (
	"runtime"
	"sync"

	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/database"
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/expire"
	"github.com/omniscale/imposm3/geom/limit"
	"github.com/omniscale/imposm3/logging"
	"github.com/omniscale/imposm3/proj"
	"github.com/omniscale/imposm3/stats"
	"github.com/omniscale/imposm3/geom/geos"
	geomp "github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/mapping"
)

var log = logging.NewLogger("writer")

type ErrorLevel interface {
	Level() int
}

type looper interface {
	loop()
}

type OsmElemWriter struct {
	osmCache   *cache.OSMCache
	diffCache  *cache.DiffCache
	progress   *stats.Statistics
	inserter   database.Inserter
	wg         *sync.WaitGroup
	limiter    *limit.Limiter
	writer     looper
	srid       int
	expireor   expire.Expireor
	concurrent bool
}

func (writer *OsmElemWriter) SetLimiter(limiter *limit.Limiter) {
	writer.limiter = limiter
}

func (writer *OsmElemWriter) EnableConcurrent() {
	writer.concurrent = true
}

func (writer *OsmElemWriter) Start() {
	concurrency := 1
	if writer.concurrent {
		concurrency = runtime.NumCPU()
	}
	for i := 0; i < concurrency; i++ {
		writer.wg.Add(1)
		go writer.writer.loop()
	}
}

func (writer *OsmElemWriter) SetExpireor(exp expire.Expireor) {
	writer.expireor = exp
}

func (writer *OsmElemWriter) Wait() {
	writer.wg.Wait()
}

func (writer *OsmElemWriter) NodesToSrid(nodes []element.Node) {
	if writer.srid == 4326 {
		return
	}
	if writer.srid != 3857 {
		panic("invalid srid. only 4326 and 3857 are supported")
	}

	for i, nd := range nodes {
		nodes[i].Long, nodes[i].Lat = proj.WgsToMerc(nd.Long, nd.Lat)
	}
}

func (writer *OsmElemWriter) NodeToSrid(node *element.Node) {
	if writer.srid == 4326 {
		return
	}
	if writer.srid != 3857 {
		panic("invalid srid. only 4326 and 3857 are supported")
	}
	node.Long, node.Lat = proj.WgsToMerc(node.Long, node.Lat)
}

func (writer *OsmElemWriter) buildAndInsertWay(g *geos.Geos, w *element.Way, matches []mapping.Match, isPolygon bool) error {
	var err error
	var geosgeom *geos.Geom
	// make copy to avoid interference with polygon/linestring matches
	way := element.Way(*w)

	if isPolygon {
		geosgeom, err = geomp.Polygon(g, way.Nodes)
	} else {
		geosgeom, err = geomp.LineString(g, way.Nodes)
	}
	if err != nil {
		return err
	}

	geom, err := geomp.AsGeomElement(g, geosgeom)
	if err != nil {
		return err
	}

	if writer.limiter != nil {
		parts, err := writer.limiter.Clip(geom.Geom)
		if err != nil {
			return err
		}
		for _, p := range parts {
			way := element.Way(*w)
			geom = geomp.Geometry{Geom: p, Wkb: g.AsEwkbHex(p)}
			if isPolygon {
				if err := writer.inserter.InsertPolygon(way.OSMElem, geom, matches); err != nil {
					return err
				}
			} else {
				if err := writer.inserter.InsertLineString(way.OSMElem, geom, matches); err != nil {
					return err
				}
			}
		}
	} else {
		if isPolygon {
			if err := writer.inserter.InsertPolygon(way.OSMElem, geom, matches); err != nil {
				return err
			}
		} else {
			if err := writer.inserter.InsertLineString(way.OSMElem, geom, matches); err != nil {
				return err
			}
		}
	}
	return nil
}