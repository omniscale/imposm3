package postgis

import (
	"database/sql"
	"fmt"
	"sync"
)

type TableTx struct {
	Pg         *PostGIS
	Tx         *sql.Tx
	Table      string
	Spec       *TableSpec
	InsertStmt *sql.Stmt
	DeleteStmt *sql.Stmt
	InsertSql  string
	DeleteSql  string
	bulkImport bool
	wg         *sync.WaitGroup
	rows       chan []interface{}
}

func NewTableTx(pg *PostGIS, spec *TableSpec, bulkImport bool) *TableTx {
	tt := &TableTx{
		Pg:         pg,
		Table:      spec.Name,
		Spec:       spec,
		wg:         &sync.WaitGroup{},
		rows:       make(chan []interface{}, 64),
		bulkImport: bulkImport,
	}
	tt.wg.Add(1)
	go tt.loop()
	return tt
}

func (tt *TableTx) Begin() error {
	tx, err := tt.Pg.Db.Begin()
	if err != nil {
		return err
	}
	tt.Tx = tx

	if tt.bulkImport {
		_, err = tx.Exec(fmt.Sprintf(`TRUNCATE TABLE "%s"."%s" RESTART IDENTITY`, tt.Pg.Schema, tt.Table))
		if err != nil {
			return err
		}
	}

	if tt.bulkImport {
		tt.InsertSql = tt.Spec.CopySQL()
	} else {
		tt.InsertSql = tt.Spec.InsertSQL()
	}

	stmt, err := tt.Tx.Prepare(tt.InsertSql)
	if err != nil {
		return &SQLError{tt.InsertSql, err}
	}
	tt.InsertStmt = stmt

	if !tt.bulkImport {
		// bulkImport creates COPY FROM STDIN stmt that doesn't
		// permit other stmt
		tt.DeleteSql = tt.Spec.DeleteSQL()
		stmt, err = tt.Tx.Prepare(tt.DeleteSql)
		if err != nil {
			return &SQLError{tt.DeleteSql, err}
		}
		tt.DeleteStmt = stmt
	}

	return nil
}

func (tt *TableTx) Insert(row []interface{}) error {
	tt.rows <- row
	return nil
}

func (tt *TableTx) loop() {
	for row := range tt.rows {
		_, err := tt.InsertStmt.Exec(row...)
		if err != nil {
			// TODO
			log.Fatal(&SQLInsertError{SQLError{tt.InsertSql, err}, row})
		}
	}
	tt.wg.Done()
}

func (tt *TableTx) Delete(id int64) error {
	if tt.bulkImport {
		panic("unable to delete in bulkImport mode")
	}
	_, err := tt.DeleteStmt.Exec(id)
	if err != nil {
		return &SQLInsertError{SQLError{tt.DeleteSql, err}, id}
	}
	return nil
}

func (tt *TableTx) Commit() error {
	close(tt.rows)
	tt.wg.Wait()
	if tt.bulkImport && tt.InsertStmt != nil {
		_, err := tt.InsertStmt.Exec()
		if err != nil {
			return err
		}
	}
	err := tt.Tx.Commit()
	if err != nil {
		return err
	}
	tt.Tx = nil
	return nil
}

func (tt *TableTx) Rollback() {
	rollbackIfTx(&tt.Tx)
}
