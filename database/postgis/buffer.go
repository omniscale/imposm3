package postgis

type InsertElement struct {
	Table string
	Row   []interface{}
}
type DeleteElement struct {
	Table string
	Id    int64
}

type InsertBuffer struct {
	insertc chan InsertElement
	deletec chan DeleteElement
	done    chan bool
	Tables  map[string]*TableTx
}

func NewInsertBuffer(pg *PostGIS, bulkImport bool) *InsertBuffer {
	ib := InsertBuffer{
		insertc: make(chan InsertElement),
		deletec: make(chan DeleteElement),
		done:    make(chan bool),
		Tables:  make(map[string]*TableTx),
	}
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
	ib.done <- true
}

func (ib *InsertBuffer) Insert(table string, row []interface{}) {
	ib.insertc <- InsertElement{table, row}
}

func (ib *InsertBuffer) Delete(table string, id int64) {
	ib.deletec <- DeleteElement{table, id}
}

func (ib *InsertBuffer) loop() {
	for {
		select {
		case elem := <-ib.insertc:
			tt, ok := ib.Tables[elem.Table]
			if !ok {
				panic("unknown table " + elem.Table)
			}
			tt.Insert(elem.Row)
		case elem := <-ib.deletec:
			tt, ok := ib.Tables[elem.Table]
			if !ok {
				panic("unknown table " + elem.Table)
			}
			tt.Delete(elem.Id)
		case <-ib.done:
			return
		}
	}
}
