package postgis

type TxRouter struct {
	Tables map[string]*TableTx
}

func newTxRouter(pg *PostGIS, bulkImport bool) *TxRouter {
	ib := TxRouter{
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

func (ib *TxRouter) End() error {
	for _, tt := range ib.Tables {
		if err := tt.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func (ib *TxRouter) Abort() error {
	for _, tt := range ib.Tables {
		tt.Rollback()
	}
	return nil
}

func (ib *TxRouter) Insert(table string, row []interface{}) {
	tt, ok := ib.Tables[table]
	if !ok {
		panic("unknown table " + table)
	}
	tt.Insert(row)
}

func (ib *TxRouter) Delete(table string, id int64) {
	tt, ok := ib.Tables[table]
	if !ok {
		panic("unknown table " + table)
	}
	tt.Delete(id)
}
