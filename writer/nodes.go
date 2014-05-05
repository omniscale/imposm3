package writer

import (
	"imposm3/cache"
	"imposm3/database"
	"imposm3/element"
	"imposm3/geom"
	"imposm3/geom/geos"
	"imposm3/mapping"
	"imposm3/proj"
	"imposm3/stats"
	"sync"
)

type NodeWriter struct {
	OsmElemWriter
	nodes        chan *element.Node
	pointMatcher *mapping.TagMatcher
}

func NewNodeWriter(
	osmCache *cache.OSMCache,
	nodes chan *element.Node,
	inserter database.Inserter,
	progress *stats.Statistics,
	matcher *mapping.TagMatcher,
	srid int,
) *OsmElemWriter {
	nw := NodeWriter{
		OsmElemWriter: OsmElemWriter{
			osmCache: osmCache,
			progress: progress,
			wg:       &sync.WaitGroup{},
			inserter: inserter,
			srid:     srid,
		},
		pointMatcher: matcher,
		nodes:        nodes,
	}
	nw.OsmElemWriter.writer = &nw
	return &nw.OsmElemWriter
}

func (nw *NodeWriter) loop() {
	geos := geos.NewGeos()
	geos.SetHandleSrid(nw.srid)
	defer geos.Finish()

	for n := range nw.nodes {
		nw.progress.AddNodes(1)
		if matches := nw.pointMatcher.Match(&n.Tags); len(matches) > 0 {
			proj.NodeToMerc(n)
			if nw.expireor != nil {
				nw.expireor.Expire(n.Long, n.Lat)
			}
			point, err := geom.Point(geos, *n)
			if err != nil {
				if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
					log.Warn(err)
				}
				continue
			}

			n.Geom, err = geom.AsGeomElement(geos, point)
			if err != nil {
				log.Warn(err)
				continue
			}

			if nw.limiter != nil {
				parts, err := nw.limiter.Clip(n.Geom.Geom)
				if err != nil {
					log.Warn(err)
					continue
				}
				if len(parts) >= 1 {
					if err := nw.inserter.InsertPoint(n.OSMElem, matches); err != nil {
						log.Warn(err)
						continue
					}
				}
			} else {
				if err := nw.inserter.InsertPoint(n.OSMElem, matches); err != nil {
					log.Warn(err)
					continue
				}
			}

		}
	}
	nw.wg.Done()
}
