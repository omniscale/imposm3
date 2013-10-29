package postgis

import (
	"database/sql"
	"fmt"
	"sync"
)

type TableTx interface {
	Begin(*sql.Tx) error
	Insert(row []interface{}) error
	Delete(id int64) error
	End()
	Commit() error
	Rollback()
}

type tableTx struct {
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

func NewTableTx(pg *PostGIS, spec *TableSpec, bulkImport bool) TableTx {
	tt := &tableTx{
		Pg:         pg,
		Table:      spec.FullName,
		Spec:       spec,
		wg:         &sync.WaitGroup{},
		rows:       make(chan []interface{}, 64),
		bulkImport: bulkImport,
	}
	tt.wg.Add(1)
	go tt.loop()
	return tt
}

func (tt *tableTx) Begin(tx *sql.Tx) error {
	var err error
	if tx == nil {
		tx, err = tt.Pg.Db.Begin()
		if err != nil {
			return err
		}
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

func (tt *tableTx) Insert(row []interface{}) error {
	tt.rows <- row
	return nil
}

func (tt *tableTx) loop() {
	for row := range tt.rows {
		_, err := tt.InsertStmt.Exec(row...)
		if err != nil {
			// TODO
			log.Fatal(&SQLInsertError{SQLError{tt.InsertSql, err}, row})
		}
	}
	tt.wg.Done()
}

func (tt *tableTx) Delete(id int64) error {
	if tt.bulkImport {
		panic("unable to delete in bulkImport mode")
	}
	_, err := tt.DeleteStmt.Exec(id)
	if err != nil {
		return &SQLInsertError{SQLError{tt.DeleteSql, err}, id}
	}
	return nil
}

func (tt *tableTx) End() {
	close(tt.rows)
	tt.wg.Wait()
}

func (tt *tableTx) Commit() error {
	tt.End()
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

func (tt *tableTx) Rollback() {
	rollbackIfTx(&tt.Tx)
}

type generalizedTableTx struct {
	Pg         *PostGIS
	Tx         *sql.Tx
	Table      string
	Spec       *GeneralizedTableSpec
	InsertStmt *sql.Stmt
	DeleteStmt *sql.Stmt
	InsertSql  string
	DeleteSql  string
}

func NewGeneralizedTableTx(pg *PostGIS, spec *GeneralizedTableSpec) TableTx {
	tt := &generalizedTableTx{
		Pg:    pg,
		Table: spec.FullName,
		Spec:  spec,
	}
	return tt
}

func (tt *generalizedTableTx) Begin(tx *sql.Tx) error {
	var err error
	if tx == nil {
		tx, err = tt.Pg.Db.Begin()
		if err != nil {
			return err
		}
	}
	tt.Tx = tx

	tt.InsertSql = tt.Spec.InsertSQL()

	stmt, err := tt.Tx.Prepare(tt.InsertSql)
	if err != nil {
		return &SQLError{tt.InsertSql, err}
	}
	tt.InsertStmt = stmt

	tt.DeleteSql = tt.Spec.DeleteSQL()
	stmt, err = tt.Tx.Prepare(tt.DeleteSql)
	if err != nil {
		return &SQLError{tt.DeleteSql, err}
	}
	tt.DeleteStmt = stmt

	return nil
}

func (tt *generalizedTableTx) Insert(row []interface{}) error {
	_, err := tt.InsertStmt.Exec(row[0])
	if err != nil {
		return &SQLInsertError{SQLError{tt.InsertSql, err}, row}
	}
	return nil
}

func (tt *generalizedTableTx) Delete(id int64) error {
	_, err := tt.DeleteStmt.Exec(id)
	if err != nil {
		return &SQLInsertError{SQLError{tt.DeleteSql, err}, id}
	}
	return nil
}

func (tt *generalizedTableTx) End() {
}

func (tt *generalizedTableTx) Commit() error {
	err := tt.Tx.Commit()
	if err != nil {
		return err
	}
	tt.Tx = nil
	return nil
}

func (tt *generalizedTableTx) Rollback() {
	rollbackIfTx(&tt.Tx)
}
