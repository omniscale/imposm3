package writer

import (
	"imposm3/cache"
	"imposm3/database"
	"imposm3/element"
	"imposm3/expire"
	"imposm3/geom/limit"
	"imposm3/mapping"
	"imposm3/stats"
	"runtime"
	"sync"
)

type ErrorLevel interface {
	Level() int
}

type looper interface {
	loop()
}

type OsmElemWriter struct {
	osmCache    *cache.OSMCache
	diffCache   *cache.DiffCache
	progress    *stats.Statistics
	inserter    database.Inserter
	wg          *sync.WaitGroup
	limiter     *limit.Limiter
	writer      looper
	srid        int
	expireTiles *expire.Tiles
}

func (writer *OsmElemWriter) SetLimiter(limiter *limit.Limiter) {
	writer.limiter = limiter
}

func (writer *OsmElemWriter) Start() {
	for i := 0; i < runtime.NumCPU(); i++ {
		writer.wg.Add(1)
		go writer.writer.loop()
	}
}

func (writer *OsmElemWriter) SetExpireTiles(expireTiles *expire.Tiles) {
	writer.expireTiles = expireTiles
}

func (writer *OsmElemWriter) Close() {
	writer.wg.Wait()
}

func (writer *OsmElemWriter) insertMatches(elem *element.OSMElem, matches []mapping.Match) {
	for _, match := range matches {
		writer.inserter.Insert(*elem, match)
	}
}
