package writer

import (
	"goposm/cache"
	"goposm/element"
	"goposm/geom"
	"goposm/geom/clipper"
	"goposm/geom/geos"
	"goposm/mapping"
	"goposm/proj"
	"goposm/stats"
	"log"
	"runtime"
	"sync"
)

type NodeWriter struct {
	osmCache     *cache.OSMCache
	nodes        chan *element.Node
	tagMatcher   *mapping.TagMatcher
	progress     *stats.Statistics
	insertBuffer *InsertBuffer
	wg           *sync.WaitGroup
	clipper      *clipper.Clipper
}

func NewNodeWriter(osmCache *cache.OSMCache, nodes chan *element.Node,
	insertBuffer *InsertBuffer, tagMatcher *mapping.TagMatcher, progress *stats.Statistics) *NodeWriter {
	nw := NodeWriter{
		osmCache:     osmCache,
		nodes:        nodes,
		insertBuffer: insertBuffer,
		tagMatcher:   tagMatcher,
		progress:     progress,
		wg:           &sync.WaitGroup{},
	}

	return &nw
}

func (nw *NodeWriter) SetClipper(clipper *clipper.Clipper) {
	nw.clipper = clipper
}

func (nw *NodeWriter) Start() {
	for i := 0; i < runtime.NumCPU(); i++ {
		nw.wg.Add(1)
		go nw.loop()
	}
}

func (nw *NodeWriter) Close() {
	nw.wg.Wait()
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
					for _, match := range matches {
						row := match.Row(&n.OSMElem)
						nw.insertBuffer.Insert(match.Table.Name, row)
					}
				}
			} else {
				for _, match := range matches {
					row := match.Row(&n.OSMElem)
					nw.insertBuffer.Insert(match.Table.Name, row)
				}
			}

		}
		// fmt.Println(r)
	}
	nw.wg.Done()
}
