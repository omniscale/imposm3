package writer

import (
	"goposm/cache"
	"goposm/database"
	"goposm/element"
	"goposm/geom/clipper"
	"goposm/mapping"
	"goposm/stats"
	"log"
	"runtime"
	"sync"
)

type ErrorLevel interface {
	Level() int
}

type DbWriter struct {
	Db database.DB
	In chan InsertBatch
	wg *sync.WaitGroup
}

func NewDbWriter(db database.DB, in chan InsertBatch) *DbWriter {
	dw := DbWriter{
		Db: db,
		In: in,
		wg: &sync.WaitGroup{},
	}
	for i := 0; i < runtime.NumCPU(); i++ {
		dw.wg.Add(1)
		go dw.loop()
	}
	return &dw
}

func (dw *DbWriter) Close() {
	dw.wg.Wait()
}

func (dw *DbWriter) loop() {
	for batch := range dw.In {
		err := dw.Db.InsertBatch(batch.Table, batch.Rows)
		if err != nil {
			log.Println(err)
		}
	}
	dw.wg.Done()
}

type looper interface {
	loop()
}

type OsmElemWriter struct {
	osmCache     *cache.OSMCache
	diffCache    *cache.DiffCache
	progress     *stats.Statistics
	insertBuffer *InsertBuffer
	wg           *sync.WaitGroup
	clipper      *clipper.Clipper
	writer       looper
}

func (writer *OsmElemWriter) SetClipper(clipper *clipper.Clipper) {
	writer.clipper = clipper
}

func (writer *OsmElemWriter) Start() {
	for i := 0; i < runtime.NumCPU(); i++ {
		writer.wg.Add(1)
		go writer.writer.loop()
	}
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
