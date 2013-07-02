package postgis

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
)

func schemasFromConnectionParams(params string) (string, string) {
	parts := strings.Fields(params)
	var schema, backupSchema string
	for _, p := range parts {
		if strings.HasPrefix(p, "schema=") {
			schema = strings.Replace(p, "schema=", "", 1)
		} else if strings.HasPrefix(p, "backupschema=") {
			backupSchema = strings.Replace(p, "backupschema=", "", 1)
		}
	}
	if schema == "" {
		schema = "import"
	}
	if backupSchema == "" {
		backupSchema = "backup"
	}
	return schema, backupSchema
}

func prefixFromConnectionParams(params string) string {
	parts := strings.Fields(params)
	var prefix string
	for _, p := range parts {
		if strings.HasPrefix(p, "prefix=") {
			prefix = strings.Replace(p, "prefix=", "", 1)
			break
		}
	}
	if prefix == "" {
		prefix = "osm_"
	}
	if prefix[len(prefix)-1] != '_' {
		prefix = prefix + "_"
	}
	return prefix
}

func tableExists(tx *sql.Tx, schema, table string) (bool, error) {
	var exists bool
	sql := fmt.Sprintf(`SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='%s' AND table_schema='%s')`,
		table, schema)
	row := tx.QueryRow(sql)
	err := row.Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func dropTableIfExists(tx *sql.Tx, schema, table string) error {
	sql := fmt.Sprintf("SELECT DropGeometryTable('%s', '%s');",
		schema, table)
	row := tx.QueryRow(sql)
	var void interface{}
	err := row.Scan(&void)
	if err != nil {
		return &SQLError{sql, err}
	}
	return nil
}

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
