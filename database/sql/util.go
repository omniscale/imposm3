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
	sqlStmt := qb.DropTableSQL(schema, table)
	row := tx.QueryRow(sqlStmt)
	var void interface{}
	err = row.Scan(&void)
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
