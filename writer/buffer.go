package writer

import (
	"sync"
)

const bufferSize = 1024 * 8

type InsertBatch struct {
	Table string
	Rows  [][]interface{}
}

type InsertElement struct {
	Table string
	Row   []interface{}
}

type InsertBuffer struct {
	In  chan InsertElement
	Out chan InsertBatch
	wg  *sync.WaitGroup
}

func NewInsertBuffer() *InsertBuffer {
	ib := InsertBuffer{
		In:  make(chan InsertElement, 256),
		Out: make(chan InsertBatch, 8),
		wg:  &sync.WaitGroup{},
	}
	ib.wg.Add(1)
	go ib.loop()
	return &ib
}

func (ib *InsertBuffer) Close() {
	close(ib.In)
	ib.wg.Wait()
	close(ib.Out)
}

func (ib *InsertBuffer) Insert(table string, row []interface{}) {
	ib.In <- InsertElement{table, row}
}

func (ib *InsertBuffer) loop() {
	buffer := make(map[string]*InsertBatch)

	for elem := range ib.In {
		if batch, ok := buffer[elem.Table]; ok {
			batch.Rows = append(batch.Rows, elem.Row)
		} else {
			buffer[elem.Table] = &InsertBatch{elem.Table, [][]interface{}{elem.Row}}
		}
		if len(buffer[elem.Table].Rows) > bufferSize {
			batch := buffer[elem.Table]
			delete(buffer, elem.Table)
			ib.Out <- *batch
		}
	}
	for table, batch := range buffer {
		delete(buffer, table)
		ib.Out <- *batch
	}
	ib.wg.Done()
}
