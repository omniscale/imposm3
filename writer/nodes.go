package writer

import (
	"goposm/cache"
	"goposm/element"
	"goposm/geom"
	"goposm/geom/geos"
	"goposm/mapping"
	"goposm/proj"
	"goposm/stats"
	"log"
	"sync"
)

type NodeWriter struct {
	OsmElemWriter
	nodes      chan *element.Node
	tagMatcher *mapping.TagMatcher
}

func NewNodeWriter(osmCache *cache.OSMCache, nodes chan *element.Node,
	insertBuffer *InsertBuffer, tagMatcher *mapping.TagMatcher, progress *stats.Statistics) *OsmElemWriter {
	nw := NodeWriter{
		OsmElemWriter: OsmElemWriter{
			osmCache:     osmCache,
			progress:     progress,
			wg:           &sync.WaitGroup{},
			insertBuffer: insertBuffer,
		},
		nodes:      nodes,
		tagMatcher: tagMatcher,
	}
	nw.OsmElemWriter.writer = &nw
	return &nw.OsmElemWriter
}

func (nw *NodeWriter) loop() {
	geos := geos.NewGeos()
	defer geos.Finish()
	var err error

	for n := range nw.nodes {
		nw.progress.AddNodes(1)
		if matches := nw.tagMatcher.Match(&n.Tags); len(matches) > 0 {
			proj.NodeToMerc(n)
			n.Geom, err = geom.PointWkb(geos, *n)
			if err != nil {
				if err, ok := err.(ErrorLevel); ok {
					if err.Level() <= 0 {
						continue
					}
				}
				log.Println(err)
				continue
			}
			if nw.clipper != nil {
				parts, err := nw.clipper.Clip(n.Geom.Geom)
				if err != nil {
					log.Println(err)
					continue
				}
				if len(parts) >= 1 {
					nw.insertMatches(&n.OSMElem, matches)
				}
			} else {
				nw.insertMatches(&n.OSMElem, matches)
			}

		}
	}
	nw.wg.Done()
}
