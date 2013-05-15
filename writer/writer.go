package writer

import (
	"goposm/database"
	"log"
)

const batchSize = 1024

func DBWriter(db database.DB, in chan InsertBatch) {
	for batch := range in {
		err := db.InsertBatch(batch.Table, batch.Rows)
		if err != nil {
			log.Println(err)
		}
	}
}

type InsertBatch struct {
	Table string
	Rows  [][]interface{}
}

func BufferInsertElements(in chan InsertElement, out chan InsertBatch) {
	buffer := make(map[string]*InsertBatch)

	for elem := range in {
		if batch, ok := buffer[elem.Table]; ok {
			batch.Rows = append(batch.Rows, elem.Row)
		} else {
			buffer[elem.Table] = &InsertBatch{elem.Table, [][]interface{}{elem.Row}}
		}
		if len(buffer[elem.Table].Rows) > batchSize {
			batch := buffer[elem.Table]
			delete(buffer, elem.Table)
			out <- *batch
		}
	}
	for table, batch := range buffer {
		delete(buffer, table)
		out <- *batch
	}
}

type InsertElement struct {
	Table string
	Row   []interface{}
}
