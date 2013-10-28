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
	Tables map[string]*TableTx
}

func NewInsertBuffer(pg *PostGIS, bulkImport bool) *InsertBuffer {
	ib := InsertBuffer{
		Tables: make(map[string]*TableTx),
	}
	for tableName, table := range pg.Tables {
		tt := pg.NewTableTx(table, bulkImport)
		err := tt.Begin()
		if err != nil {
			panic(err) // TODO
		}
		ib.Tables[tableName] = tt
	}

	return &ib
}

func (ib *InsertBuffer) End() error {
	for _, tt := range ib.Tables {
		if err := tt.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func (ib *InsertBuffer) Abort() error {
	for _, tt := range ib.Tables {
		tt.Rollback()
	}
	return nil
}

func (ib *InsertBuffer) Insert(table string, row []interface{}) {
	tt, ok := ib.Tables[table]
	if !ok {
		panic("unknown table " + table)
	}
	tt.Insert(row)
}

func (ib *InsertBuffer) Delete(table string, id int64) {
	tt, ok := ib.Tables[table]
	if !ok {
		panic("unknown table " + table)
	}
	tt.Delete(id)
}
