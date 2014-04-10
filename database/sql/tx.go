package sql

import (
	"database/sql"
	"sync"
)

type tableSpec interface {
	InsertSQL() string
	DeleteSQL() string
}

type bulkTableSpec interface {
  tableSpec
  CopySQL() string
}

type TableTx interface {
	Begin(*sql.Tx) error
	Insert(row []interface{}) error
	Delete(id int64) error
	End()
	Commit() error
	Rollback()
}

type bulkTableTx struct {
	Sdb        *SQLDB
	Tx         *sql.Tx
	Table      string
	Spec       bulkTableSpec
	InsertStmt *sql.Stmt
	InsertSql  string
	wg         *sync.WaitGroup
	rows       chan []interface{}
}

func NewBulkTableTx(sdb *SQLDB, tableName string, spec bulkTableSpec) TableTx {
	tt := &bulkTableTx{
		Sdb:    sdb,
		Table: tableName,
		Spec:  spec,
		wg:    &sync.WaitGroup{},
		rows:  make(chan []interface{}, 64),
	}
	tt.wg.Add(1)
	go tt.loop()
	return tt
}

func (tt *bulkTableTx) Begin(tx *sql.Tx) error {
	var err error
	if tx == nil {
		tx, err = tt.Sdb.Db.Begin()
		if err != nil {
			return err
		}
	}
	tt.Tx = tx

	_, err = tx.Exec(tt.Sdb.QB.TruncateTableSQL(tt.Sdb.Config.ImportSchema, tt.Table))
	if err != nil {
		return err
	}

	tt.InsertSql = tt.Spec.CopySQL()

	stmt, err := tt.Tx.Prepare(tt.InsertSql)
	if err != nil {
		return &SQLError{tt.InsertSql, err}
	}
	tt.InsertStmt = stmt

	return nil
}

func (tt *bulkTableTx) Insert(row []interface{}) error {
	tt.rows <- row
	return nil
}

func (tt *bulkTableTx) loop() {
	for row := range tt.rows {
		_, err := tt.InsertStmt.Exec(row...)
		if err != nil {
			// TODO
			log.Fatal(&SQLInsertError{SQLError{tt.InsertSql, err}, row})
		}
	}
	tt.wg.Done()
}

func (tt *bulkTableTx) Delete(id int64) error {
	panic("unable to delete in bulkImport mode")
}

func (tt *bulkTableTx) End() {
	close(tt.rows)
	tt.wg.Wait()
}

func (tt *bulkTableTx) Commit() error {
	tt.End()
	if tt.InsertStmt != nil {
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

func (tt *bulkTableTx) Rollback() {
	rollbackIfTx(&tt.Tx)
}

type syncTableTx struct {
	Sdb        *SQLDB
	Tx         *sql.Tx
	Table      string
	Spec       tableSpec
	InsertStmt *sql.Stmt
	DeleteStmt *sql.Stmt
	InsertSql  string
	DeleteSql  string
}

func NewSynchronousTableTx(sdb *SQLDB, tableName string, spec tableSpec) TableTx {
	tt := &syncTableTx{
		Sdb:    sdb,
		Table: tableName,
		Spec:  spec,
	}
	return tt
}

func (tt *syncTableTx) Begin(tx *sql.Tx) error {
	var err error
	if tx == nil {
		tx, err = tt.Sdb.Db.Begin()
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

func (tt *syncTableTx) Insert(row []interface{}) error {
	_, err := tt.InsertStmt.Exec(row...)
	if err != nil {
		return &SQLInsertError{SQLError{tt.InsertSql, err}, row}
	}
	return nil
}

func (tt *syncTableTx) Delete(id int64) error {
	_, err := tt.DeleteStmt.Exec(id)
	if err != nil {
		return &SQLInsertError{SQLError{tt.DeleteSql, err}, id}
	}
	return nil
}

func (tt *syncTableTx) End() {
}

func (tt *syncTableTx) Commit() error {
	err := tt.Tx.Commit()
	if err != nil {
		return err
	}
	tt.Tx = nil
	return nil
}

func (tt *syncTableTx) Rollback() {
	rollbackIfTx(&tt.Tx)
}
