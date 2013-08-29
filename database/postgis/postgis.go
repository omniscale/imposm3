package postgis

import (
	"database/sql"
	"errors"
	"fmt"
	pq "github.com/olt/pq"
	"imposm3/database"
	"imposm3/logging"
	"imposm3/mapping"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
)

var log = logging.NewLogger("PostGIS")

type SQLError struct {
	query         string
	originalError error
}

func (e *SQLError) Error() string {
	return fmt.Sprintf("SQL Error: %s in query %s", e.originalError.Error(), e.query)
}

type SQLInsertError struct {
	SQLError
	data interface{}
}

func (e *SQLInsertError) Error() string {
	return fmt.Sprintf("SQL Error: %s in query %s (%+v)", e.originalError.Error(), e.query, e.data)
}

func createTable(tx *sql.Tx, spec TableSpec) error {
	var sql string
	var err error

	err = dropTableIfExists(tx, spec.Schema, spec.Name)
	if err != nil {
		return err
	}

	sql = spec.CreateTableSQL()
	_, err = tx.Exec(sql)
	if err != nil {
		return &SQLError{sql, err}
	}

	err = addGeometryColumn(tx, spec.Name, spec)
	if err != nil {
		return err
	}
	return nil
}

func addGeometryColumn(tx *sql.Tx, tableName string, spec TableSpec) error {
	geomType := strings.ToUpper(spec.GeometryType)
	if geomType == "POLYGON" {
		geomType = "GEOMETRY" // for multipolygon support
	}
	sql := fmt.Sprintf("SELECT AddGeometryColumn('%s', '%s', 'geometry', '%d', '%s', 2);",
		spec.Schema, tableName, spec.Srid, geomType)
	row := tx.QueryRow(sql)
	var void interface{}
	err := row.Scan(&void)
	if err != nil {
		return &SQLError{sql, err}
	}
	return nil
}

func populateGeometryColumn(tx *sql.Tx, tableName string, spec TableSpec) error {
	sql := fmt.Sprintf("SELECT Populate_Geometry_Columns('%s.%s'::regclass);",
		spec.Schema, tableName)
	row := tx.QueryRow(sql)
	var void interface{}
	err := row.Scan(&void)
	if err != nil {
		return &SQLError{sql, err}
	}
	return nil
}

func (pg *PostGIS) createSchema(schema string) error {
	var sql string
	var err error

	if schema == "public" {
		return nil
	}

	sql = fmt.Sprintf("SELECT EXISTS(SELECT schema_name FROM information_schema.schemata WHERE schema_name = '%s');",
		schema)
	row := pg.Db.QueryRow(sql)
	var exists bool
	err = row.Scan(&exists)
	if err != nil {
		return &SQLError{sql, err}
	}
	if exists {
		return nil
	}

	sql = fmt.Sprintf("CREATE SCHEMA \"%s\"", schema)
	_, err = pg.Db.Exec(sql)
	if err != nil {
		return &SQLError{sql, err}
	}
	return nil
}

func (pg *PostGIS) InsertBatch(table string, rows [][]interface{}) error {
	spec, ok := pg.Tables[table]
	if !ok {
		return errors.New("unkown table: " + table)
	}

	tx, err := pg.Db.Begin()
	if err != nil {
		return err
	}
	defer rollbackIfTx(&tx)

	sql := spec.InsertSQL()
	stmt, err := tx.Prepare(sql)
	if err != nil {
		return &SQLError{sql, err}
	}
	defer stmt.Close()

	for _, row := range rows {
		_, err := stmt.Exec(row...)
		if err != nil {
			return &SQLInsertError{SQLError{sql, err}, row}
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	tx = nil // set nil to prevent rollback
	return nil
}

func (pg *PostGIS) Init() error {
	if err := pg.createSchema(pg.Schema); err != nil {
		return err
	}

	tx, err := pg.Db.Begin()
	if err != nil {
		return err
	}
	defer rollbackIfTx(&tx)
	for _, spec := range pg.Tables {
		if err := createTable(tx, *spec); err != nil {
			return err
		}
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	tx = nil
	return nil
}

func (pg *PostGIS) TableNames() []string {
	var names []string
	for name, _ := range pg.Tables {
		names = append(names, name)
	}
	for name, _ := range pg.GeneralizedTables {
		names = append(names, name)
	}
	return names
}

// Finish creates spatial indices on all tables.
func (pg *PostGIS) Finish() error {
	defer log.StopStep(log.StartStep(fmt.Sprintf("Creating geometry indices")))

	worker := int(runtime.NumCPU() / 2)
	if worker < 1 {
		worker = 1
	}

	p := newWorkerPool(worker, len(pg.Tables)+len(pg.GeneralizedTables))
	for tableName, tbl := range pg.Tables {
		tableName := pg.Prefix + tableName
		table := tbl
		p.in <- func() error {
			return createIndex(pg, tableName, table.Columns)
		}
	}

	for tableName, tbl := range pg.GeneralizedTables {
		tableName := pg.Prefix + tableName
		table := tbl
		p.in <- func() error {
			return createIndex(pg, tableName, table.Source.Columns)
		}
	}

	err := p.wait()
	if err != nil {
		return err
	}

	return nil
}

func createIndex(pg *PostGIS, tableName string, columns []ColumnSpec) error {
	for _, col := range columns {
		if col.Type.Name() == "GEOMETRY" {
			sql := fmt.Sprintf(`CREATE INDEX "%s_geom" ON "%s"."%s" USING GIST ("%s")`,
				tableName, pg.Schema, tableName, col.Name)
			step := log.StartStep(fmt.Sprintf("Creating geometry index on %s", tableName))
			_, err := pg.Db.Exec(sql)
			log.StopStep(step)
			if err != nil {
				return err
			}
		}
		if col.FieldType.Name == "id" {
			sql := fmt.Sprintf(`CREATE INDEX "%s_osm_id_idx" ON "%s"."%s" USING BTREE ("%s")`,
				tableName, pg.Schema, tableName, col.Name)
			step := log.StartStep(fmt.Sprintf("Creating OSM id index on %s", tableName))
			_, err := pg.Db.Exec(sql)
			log.StopStep(step)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (pg *PostGIS) Generalize() error {
	defer log.StopStep(log.StartStep(fmt.Sprintf("Creating generalized tables")))

	worker := int(runtime.NumCPU() / 2)
	if worker < 1 {
		worker = 1
	}
	// generalized tables can depend on other generalized tables
	// create tables with non-generalized sources first
	p := newWorkerPool(worker, len(pg.GeneralizedTables))
	for _, table := range pg.GeneralizedTables {
		if table.SourceGeneralized == nil {
			tbl := table // for following closure
			p.in <- func() error {
				if err := pg.generalizeTable(tbl); err != nil {
					return err
				}
				tbl.created = true
				return nil
			}
		}
	}
	err := p.wait()
	if err != nil {
		return err
	}

	// next create tables with created generalized sources until
	// no new source is created
	created := int32(1)
	for created == 1 {
		created = 0

		p := newWorkerPool(worker, len(pg.GeneralizedTables))
		for _, table := range pg.GeneralizedTables {
			if !table.created && table.SourceGeneralized.created {
				tbl := table // for following closure
				p.in <- func() error {
					if err := pg.generalizeTable(tbl); err != nil {
						return err
					}
					tbl.created = true
					atomic.StoreInt32(&created, 1)
					return nil
				}
			}
		}
		err := p.wait()
		if err != nil {
			return err
		}
	}
	return nil
}

func (pg *PostGIS) generalizeTable(table *GeneralizedTableSpec) error {
	defer log.StopStep(log.StartStep(fmt.Sprintf("Generalizing %s into %s",
		pg.Prefix+table.SourceName, pg.Prefix+table.Name)))

	tx, err := pg.Db.Begin()
	if err != nil {
		return err
	}
	defer rollbackIfTx(&tx)

	var where string
	if table.Where != "" {
		where = " WHERE " + table.Where
	}
	var cols []string

	for _, col := range table.Source.Columns {
		cols = append(cols, col.Type.GeneralizeSql(&col, table))
	}

	if err := dropTableIfExists(tx, pg.Schema, table.Name); err != nil {
		return err
	}

	columnSQL := strings.Join(cols, ",\n")
	sql := fmt.Sprintf(`CREATE TABLE "%s"."%s" AS (SELECT %s FROM "%s"."%s"%s)`,
		pg.Schema, table.Name, columnSQL, pg.Schema,
		pg.Prefix+table.SourceName, where)

	_, err = tx.Exec(sql)
	if err != nil {
		return &SQLError{sql, err}
	}

	err = populateGeometryColumn(tx, table.Name, *table.Source)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	tx = nil // set nil to prevent rollback
	return nil
}

// Optimize clusters tables on new GeoHash index.
func (pg *PostGIS) Optimize() error {
	defer log.StopStep(log.StartStep(fmt.Sprintf("Clustering on geometry")))

	worker := int(runtime.NumCPU() / 2)
	if worker < 1 {
		worker = 1
	}

	p := newWorkerPool(worker, len(pg.Tables)+len(pg.GeneralizedTables))

	for tableName, tbl := range pg.Tables {
		tableName := pg.Prefix + tableName
		table := tbl
		p.in <- func() error {
			return clusterTable(pg, tableName, table.Srid, table.Columns)
		}
	}
	for tableName, tbl := range pg.GeneralizedTables {
		tableName := pg.Prefix + tableName
		table := tbl
		p.in <- func() error {
			return clusterTable(pg, tableName, table.Source.Srid, table.Source.Columns)
		}
	}

	err := p.wait()
	if err != nil {
		return err
	}

	return nil
}

func clusterTable(pg *PostGIS, tableName string, srid int, columns []ColumnSpec) error {
	for _, col := range columns {
		if col.Type.Name() == "GEOMETRY" {
			step := log.StartStep(fmt.Sprintf("Indexing %s on geohash", tableName))
			sql := fmt.Sprintf(`CREATE INDEX "%s_geom_geohash" ON "%s"."%s" (ST_GeoHash(ST_Transform(ST_SetSRID(Box2D(%s), %d), 4326)))`,
				tableName, pg.Schema, tableName, col.Name, srid)
			_, err := pg.Db.Exec(sql)
			log.StopStep(step)
			if err != nil {
				return err
			}

			step = log.StartStep(fmt.Sprintf("Clustering %s on geohash", tableName))
			sql = fmt.Sprintf(`CLUSTER "%s_geom_geohash" ON "%s"."%s"`,
				tableName, pg.Schema, tableName)
			_, err = pg.Db.Exec(sql)
			log.StopStep(step)
			if err != nil {
				return err
			}
			break
		}
	}
	return nil
}

type PostGIS struct {
	Db                *sql.DB
	Params            string
	Schema            string
	BackupSchema      string
	Config            database.Config
	Tables            map[string]*TableSpec
	GeneralizedTables map[string]*GeneralizedTableSpec
	Prefix            string
	InputBuffer       *InsertBuffer
}

func (pg *PostGIS) Open() error {
	var err error

	pg.Db, err = sql.Open("postgres", pg.Params)
	if err != nil {
		return err
	}
	// check that the connection actually works
	err = pg.Db.Ping()
	if err != nil {
		return err
	}
	return nil
}

func (pg *PostGIS) Insert(table string, row []interface{}) {
	pg.InputBuffer.Insert(table, row)
}

func (pg *PostGIS) Delete(table string, id int64) error {
	pg.InputBuffer.Delete(table, id)
	return nil
}

func (pg *PostGIS) Begin() error {
	pg.InputBuffer = NewInsertBuffer(pg, false)
	return nil
}

func (pg *PostGIS) BeginBulk() error {
	pg.InputBuffer = NewInsertBuffer(pg, true)
	return nil
}

func (pg *PostGIS) Abort() error {
	return pg.InputBuffer.Abort()
}

func (pg *PostGIS) End() error {
	return pg.InputBuffer.End()
}

func (pg *PostGIS) Close() error {
	return pg.Db.Close()
}

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

func (pg *PostGIS) NewTableTx(spec *TableSpec, bulkImport bool) *TableTx {
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

func New(conf database.Config, m *mapping.Mapping) (database.DB, error) {
	db := &PostGIS{}

	db.Tables = make(map[string]*TableSpec)
	db.GeneralizedTables = make(map[string]*GeneralizedTableSpec)

	db.Config = conf

	if strings.HasPrefix(db.Config.ConnectionParams, "postgis://") {
		db.Config.ConnectionParams = strings.Replace(
			db.Config.ConnectionParams,
			"postgis", "postgres", 1,
		)
	}

	params, err := pq.ParseURL(db.Config.ConnectionParams)
	if err != nil {
		return nil, err
	}
	params = disableDefaultSslOnLocalhost(params)
	db.Schema, db.BackupSchema = schemasFromConnectionParams(params)
	db.Prefix = prefixFromConnectionParams(params)

	for name, table := range m.Tables {
		db.Tables[name] = NewTableSpec(db, table)
	}
	for name, table := range m.GeneralizedTables {
		db.GeneralizedTables[name] = NewGeneralizedTableSpec(db, table)
	}
	db.prepareGeneralizedTableSources()

	db.Params = params
	err = db.Open()
	if err != nil {
		return nil, err
	}
	return db, nil
}

// prepareGeneralizedTableSources checks if all generalized table have an
// existing source and sets .Source to the original source (works even
// when source is allready generalized).
func (pg *PostGIS) prepareGeneralizedTableSources() {
	for name, table := range pg.GeneralizedTables {
		if source, ok := pg.Tables[table.SourceName]; ok {
			table.Source = source
		} else if source, ok := pg.GeneralizedTables[table.SourceName]; ok {
			table.SourceGeneralized = source
		} else {
			log.Printf("missing source '%s' for generalized table '%s'\n",
				table.SourceName, name)
		}
	}

	// set source table until all generalized tables have a source
	for filled := true; filled; {
		filled = false
		for _, table := range pg.GeneralizedTables {
			if table.Source == nil {
				if source, ok := pg.GeneralizedTables[table.SourceName]; ok && source.Source != nil {
					table.Source = source.Source
				}
				filled = true
			}
		}
	}
}

func init() {
	database.Register("postgres", New)
	database.Register("postgis", New)
}
