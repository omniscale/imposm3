package writer

import (
	"goposm/cache"
	"goposm/database"
	"goposm/element"
	"goposm/expire"
	"goposm/geom/limit"
	"goposm/mapping"
	"goposm/stats"
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
	osmCache     *cache.OSMCache
	diffCache    *cache.DiffCache
	progress     *stats.Statistics
	insertBuffer database.RowInserter
	wg           *sync.WaitGroup
	limiter      *limit.Limiter
	writer       looper
	srid         int
	expireTiles  *expire.Tiles
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
		row := match.Row(elem)
		writer.insertBuffer.Insert(match.Table.Name, row)
	}
}
