package postgis

import (
	"database/sql"
)

// TxRouter routes inserts/deletes to TableTx
type TxRouter struct {
	Tables map[string]TableTx
	tx     *sql.Tx
}

func newTxRouter(pg *PostGIS, bulkImport bool) *TxRouter {
	txr := TxRouter{
		Tables: make(map[string]TableTx),
	}

	var tx *sql.Tx
	var err error
	if !bulkImport {
		tx, err = pg.Db.Begin()
		if err != nil {
			panic(err) // TODO
		}
		txr.tx = tx
	}
	for tableName, table := range pg.Tables {
		tt := NewTableTx(pg, table, bulkImport)
		err := tt.Begin(tx)
		if err != nil {
			panic(err) // TODO
		}
		txr.Tables[tableName] = tt
	}
	if !bulkImport {
		for tableName, table := range pg.GeneralizedTables {
			tt := NewGeneralizedTableTx(pg, table)
			err := tt.Begin(tx)
			if err != nil {
				panic(err) // TODO
			}
			txr.Tables[tableName] = tt
		}

	}
	return &txr
}

func (txr *TxRouter) End() error {
	if txr.tx != nil {
		for _, tt := range txr.Tables {
			tt.End()
		}
		return txr.tx.Commit()
	}

	for _, tt := range txr.Tables {
		if err := tt.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func (txr *TxRouter) Abort() error {
	if txr.tx != nil {
		for _, tt := range txr.Tables {
			tt.End()
		}
		return txr.tx.Rollback()
	}
	for _, tt := range txr.Tables {
		tt.Rollback()
	}
	return nil
}

func (txr *TxRouter) Insert(table string, row []interface{}) {
	tt, ok := txr.Tables[table]
	if !ok {
		panic("unknown table " + table)
	}
	tt.Insert(row)
}

func (txr *TxRouter) Delete(table string, id int64) {
	tt, ok := txr.Tables[table]
	if !ok {
		panic("unknown table " + table)
	}
	tt.Delete(id)
}
