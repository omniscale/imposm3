package writer

import (
	"goposm/database"
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
