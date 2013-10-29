package postgis

import (
	"database/sql"
)

type TxRouter struct {
	Tables map[string]TableTx
	tx     *sql.Tx
}

func newTxRouter(pg *PostGIS, bulkImport bool) *TxRouter {
	ib := TxRouter{
		Tables: make(map[string]TableTx),
	}

	var tx *sql.Tx
	var err error
	if !bulkImport {
		tx, err = pg.Db.Begin()
		if err != nil {
			panic(err) // TODO
		}
		ib.tx = tx
	}
	for tableName, table := range pg.Tables {
		tt := NewTableTx(pg, table, bulkImport)
		err := tt.Begin(tx)
		if err != nil {
			panic(err) // TODO
		}
		ib.Tables[tableName] = tt
	}
	if !bulkImport {
		for tableName, table := range pg.GeneralizedTables {
			tt := NewGeneralizedTableTx(pg, table)
			err := tt.Begin(tx)
			if err != nil {
				panic(err) // TODO
			}
			ib.Tables[tableName] = tt
		}

	}
	return &ib
}

func (ib *TxRouter) End() error {
	if ib.tx != nil {
		for _, tt := range ib.Tables {
			tt.End()
		}
		return ib.tx.Commit()
	}

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
