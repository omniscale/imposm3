package postgis

import (
	"sync"
)

type InsertElement struct {
	Table string
	Row   []interface{}
}

type InsertBuffer struct {
	In     chan InsertElement
	Tables map[string]*TableTx
	wg     *sync.WaitGroup
}

func NewInsertBuffer(pg *PostGIS, bulkImport bool) *InsertBuffer {
	ib := InsertBuffer{
		In:     make(chan InsertElement),
		Tables: make(map[string]*TableTx),
		wg:     &sync.WaitGroup{},
	}
	ib.wg.Add(1)
	for tableName, table := range pg.Tables {
		tt := pg.NewTableTx(table, bulkImport)
		err := tt.Begin()
		if err != nil {
			panic(err) // TODO
		}
		ib.Tables[tableName] = tt
	}

	go ib.loop()
	return &ib
}

func (ib *InsertBuffer) End() error {
	ib.Close()
	for _, tt := range ib.Tables {
		if err := tt.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func (ib *InsertBuffer) Abort() error {
	ib.Close()
	for _, tt := range ib.Tables {
		tt.Rollback()
	}
	return nil
}

func (ib *InsertBuffer) Close() {
	close(ib.In)
	ib.wg.Wait()
}

func (ib *InsertBuffer) Insert(table string, row []interface{}) {
	ib.In <- InsertElement{table, row}
}

func (ib *InsertBuffer) loop() {
	for elem := range ib.In {
		tt, ok := ib.Tables[elem.Table]
		if !ok {
			panic("unknown table " + elem.Table)
		}
		tt.Insert(elem.Row)
	}
	ib.wg.Done()
}
