package postgis

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/omniscale/imposm3/log"
)

type TableTx interface {
	Begin(*sql.Tx) error
	Insert(row []interface{}) error
	Delete(id int64) error
	End()
	Commit() error
	Rollback()
}

type bulkTableTx struct {
	Pg         *PostGIS
	Tx         *sql.Tx
	Table      string
	Spec       *TableSpec
	InsertStmt *sql.Stmt
	InsertSQL  string
	wg         *sync.WaitGroup
	rows       chan []interface{}
}

func NewBulkTableTx(pg *PostGIS, spec *TableSpec) TableTx {
	tt := &bulkTableTx{
		Pg:    pg,
		Table: spec.FullName,
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
		tx, err = tt.Pg.Db.Begin()
		if err != nil {
			return err
		}
	}
	tt.Tx = tx

	_, err = tx.Exec(fmt.Sprintf(`TRUNCATE TABLE "%s"."%s" RESTART IDENTITY`, tt.Pg.Config.ImportSchema, tt.Table))
	if err != nil {
		return err
	}

	tt.InsertSQL = tt.Spec.CopySQL()

	stmt, err := tt.Tx.Prepare(tt.InsertSQL)
	if err != nil {
		return &SQLError{tt.InsertSQL, err}
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
			// InsertStmt uses COPY so the error may not be related to this row.
			// Abort the import as the whole transaction is lost anyway.
			log.Fatalf("[fatal] bulk insert into %q: %s", tt.Table, &SQLError{tt.InsertSQL, err})
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
	Pg         *PostGIS
	Tx         *sql.Tx
	Table      string
	Spec       tableSpec
	InsertStmt *sql.Stmt
	DeleteStmt *sql.Stmt
	InsertSQL  string
	DeleteSQL  string
}

type tableSpec interface {
	InsertSQL() string
	DeleteSQL() string
}

func NewSynchronousTableTx(pg *PostGIS, tableName string, spec tableSpec) TableTx {
	tt := &syncTableTx{
		Pg:    pg,
		Table: tableName,
		Spec:  spec,
	}
	return tt
}

func (tt *syncTableTx) Begin(tx *sql.Tx) error {
	var err error
	if tx == nil {
		tx, err = tt.Pg.Db.Begin()
		if err != nil {
			return err
		}
	}
	tt.Tx = tx

	tt.InsertSQL = tt.Spec.InsertSQL()

	stmt, err := tt.Tx.Prepare(tt.InsertSQL)
	if err != nil {
		return &SQLError{tt.InsertSQL, err}
	}
	tt.InsertStmt = stmt

	tt.DeleteSQL = tt.Spec.DeleteSQL()
	stmt, err = tt.Tx.Prepare(tt.DeleteSQL)
	if err != nil {
		return &SQLError{tt.DeleteSQL, err}
	}
	tt.DeleteStmt = stmt

	return nil
}

func (tt *syncTableTx) Insert(row []interface{}) error {
	_, err := tt.InsertStmt.Exec(row...)
	if err != nil {
		return &SQLInsertError{SQLError{tt.InsertSQL, err}, row}
	}
	return nil
}

func (tt *syncTableTx) Delete(id int64) error {
	_, err := tt.DeleteStmt.Exec(id)
	if err != nil {
		return &SQLInsertError{SQLError{tt.DeleteSQL, err}, id}
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
