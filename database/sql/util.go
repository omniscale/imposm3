package sql

import (
	"database/sql"
	"sync"
)

func tableExists(tx *sql.Tx, qb QueryBuilder, schema, table string) (bool, error) {
	var exists bool
	row := tx.QueryRow(qb.TableExistsSQL(schema, table))
	err := row.Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func dropTableIfExists(tx *sql.Tx, qb QueryBuilder, schema, table string) error {
	exists, err := tableExists(tx, qb, schema, table)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	sqlStmt := qb.GeometryIndexesSQL(schema, table)

	// this operation is optional
	if sqlStmt != "" {
		rows, err := tx.Query(sqlStmt)
		if err != nil {
			return &SQLError{sqlStmt, err}
		}

		// remove spatial indexes

		// The query result must be cached in a slice
		// to close the query cursor, thus avoiding
		// the blocking of tables about to be deleted
		indexes := []string{}
		for rows.Next() {
			var index string
			if err := rows.Scan(&index); err != nil {
				return &SQLError{sqlStmt, err}
			}
			indexes = append(indexes, index)
		}

		for _, index := range indexes {
			// disable spatial index
			sqlStmt = qb.DisableGeometryIndexSQL(schema, table, index)
			_, err = tx.Exec(sqlStmt)
			if err != nil {
				return &SQLError{sqlStmt, err}
			}

			sqlStmt = qb.DropGeometryIndexSQL(schema, table, index)
			_, err = tx.Exec(sqlStmt)
			if err != nil {
				return &SQLError{sqlStmt, err}
			}
		}
	}

	// remove geometric columns
	row := tx.QueryRow(qb.GeometryColumnExistsSQL(schema, table))
	err = row.Scan(&exists)
	if err != nil {
		return err
	}

	if exists {
		// drop geometry column
		sqlStmt := qb.DropGeometryColumnSQL(schema, table)
		_, err = tx.Exec(sqlStmt)
		if err != nil {
			return &SQLError{sqlStmt, err}
		}
	}

	sqlStmt = qb.DropTableSQL(schema, table)
	_, err = tx.Exec(sqlStmt)
	if err != nil {
		return &SQLError{sqlStmt, err}
	}

	return nil
}

// rollbackIfTx rollsback transaction if tx is not nil.
func rollbackIfTx(tx **sql.Tx) {
	if *tx != nil {
		if err := tx.Rollback(); err != nil {
			log.Fatal("rollback failed", err)
		}
	}
}

// workerPool runs functions in n (worker) parallel goroutines.
// wait will return the first error or nil when all functions
// returned succesfull.
type workerPool struct {
	in  chan func() error
	out chan error
	wg  *sync.WaitGroup
}

func newWorkerPool(worker, tasks int) *workerPool {
	p := &workerPool{
		make(chan func() error, tasks),
		make(chan error, tasks),
		&sync.WaitGroup{},
	}
	for i := 0; i < worker; i++ {
		p.wg.Add(1)
		go p.workerLoop()
	}
	return p
}

func (p *workerPool) workerLoop() {
	for f := range p.in {
		p.out <- f()
	}
	p.wg.Done()
}

func (p *workerPool) wait() error {
	close(p.in)
	done := make(chan bool)
	go func() {
		p.wg.Wait()
		done <- true
	}()

	for {
		select {
		case err := <-p.out:
			if err != nil {
				return err
			}
		case <-done:
			return nil
		}
	}
}
