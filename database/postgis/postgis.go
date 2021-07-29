package postgis

import (
	"database/sql"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	pq "github.com/lib/pq"
	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/database"
	"github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/log"
	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/mapping/config"
	"github.com/pkg/errors"
)

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

	err = dropTableIfExists(tx, spec.Schema, spec.FullName)
	if err != nil {
		return err
	}

	sql = spec.CreateTableSQL()
	_, err = tx.Exec(sql)
	if err != nil {
		return &SQLError{sql, err}
	}

	err = addGeometryColumn(tx, spec.FullName, spec)
	if err != nil {
		return err
	}
	return nil
}

func addGeometryColumn(tx *sql.Tx, tableName string, spec TableSpec) error {
	colName := ""
	for _, col := range spec.Columns {
		if col.Type.Name() == "GEOMETRY" {
			colName = col.Name
			break
		}
	}

	if colName == "" {
		return nil
	}

	geomType := strings.ToUpper(spec.GeometryType)
	if geomType == "POLYGON" {
		geomType = "GEOMETRY" // for multipolygon support
	}
	sql := fmt.Sprintf("SELECT AddGeometryColumn('%s', '%s', '%s', '%d', '%s', 2);",
		spec.Schema, tableName, colName, spec.Srid, geomType)
	row := tx.QueryRow(sql)
	var void interface{}
	err := row.Scan(&void)
	if err != nil {
		return &SQLError{sql, err}
	}
	return nil
}

func getPostgisVersion(tx *sql.Tx) (string, error) {
	sql := fmt.Sprintf("SELECT PostGIS_lib_version();")
	row := tx.QueryRow(sql)
	var version string
	err := row.Scan(&version)
	if err != nil {
		return "", &SQLError{sql, err}
	}
	return version, nil
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

// Init creates schema and tables, drops existing data.
func (pg *PostGIS) Init() error {
	if err := pg.createSchema(pg.Config.ImportSchema); err != nil {
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

// Finish creates spatial indices on all tables.
func (pg *PostGIS) Finish() error {
	defer log.Step("Creating geometry indices")()

	worker := int(runtime.GOMAXPROCS(0))
	if worker < 1 {
		worker = 1
	}

	p := newWorkerPool(worker, len(pg.Tables)+len(pg.GeneralizedTables))
	for _, tbl := range pg.Tables {
		tableName := tbl.FullName
		table := tbl
		p.in <- func() error {
			return createIndex(pg, tableName, table.Columns, false)
		}
	}

	for _, tbl := range pg.GeneralizedTables {
		tableName := tbl.FullName
		table := tbl
		p.in <- func() error {
			return createIndex(pg, tableName, table.Source.Columns, true)
		}
	}

	err := p.wait()
	if err != nil {
		return err
	}

	return nil
}

func createIndex(pg *PostGIS, tableName string, columns []ColumnSpec, generalizedTable bool) error {
	foundIDCol := false
	for _, cs := range columns {
		if cs.Name == "id" {
			foundIDCol = true
		}
	}

	for _, col := range columns {
		if col.Type.Name() == "GEOMETRY" {
			sql := fmt.Sprintf(`CREATE INDEX "%s_geom" ON "%s"."%s" USING GIST ("%s")`,
				tableName, pg.Config.ImportSchema, tableName, col.Name)
			step := log.Step(fmt.Sprintf("Creating geometry index on %s", tableName))
			_, err := pg.Db.Exec(sql)
			step()
			if err != nil {
				return err
			}
		}
		if col.FieldType.Name == "id" && (foundIDCol || generalizedTable) {
			// Create index for OSM ID required for diff updates, but only if
			// the table does have an `id` column.
			// The explicit `id` column prevented the creation of our composite
			// PRIMARY KEY index of id (serial) and OSM ID.
			// Generalized tables also do not have a PRIMARY KEY.
			sql := fmt.Sprintf(`CREATE INDEX "%s_%s_idx" ON "%s"."%s" USING BTREE ("%s")`,
				tableName, col.Name, pg.Config.ImportSchema, tableName, col.Name)
			step := log.Step(fmt.Sprintf("Creating OSM id index on %s", tableName))
			_, err := pg.Db.Exec(sql)
			step()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (pg *PostGIS) GeneralizeUpdates() error {
	defer log.Step("Updating generalized tables")()
	for _, table := range pg.sortedGeneralizedTables() {
		if ids, ok := pg.updatedIDs[table]; ok {
			for _, id := range ids {
				pg.txRouter.Insert(table, []interface{}{id})
			}
		}
	}
	return nil
}

func (pg *PostGIS) Generalize() error {
	defer log.Step("Creating generalized tables")()

	worker := int(runtime.GOMAXPROCS(0))
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
	defer log.Step(fmt.Sprintf("Generalizing %s into %s",
		table.Source.FullName, table.FullName))()

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
		cols = append(cols, col.Type.GeneralizeSQL(&col, table))
	}

	if err := dropTableIfExists(tx, pg.Config.ImportSchema, table.FullName); err != nil {
		return errors.Wrap(err, "dropping existing table")
	}

	columnSQL := strings.Join(cols, ",\n")

	var sourceTable string
	if table.SourceGeneralized != nil {
		sourceTable = table.SourceGeneralized.FullName
	} else {
		sourceTable = table.Source.FullName
	}
	sql := fmt.Sprintf(`CREATE TABLE "%s"."%s" AS (SELECT %s FROM "%s"."%s"%s)`,
		pg.Config.ImportSchema, table.FullName, columnSQL, pg.Config.ImportSchema,
		sourceTable, where)

	_, err = tx.Exec(sql)
	if err != nil {
		return &SQLError{sql, err}
	}

	postgisVersion, err := getPostgisVersion(tx)
	if err != nil {
		return errors.Wrap(err, "detecting PostGIS version")
	}
	if strings.HasPrefix(postgisVersion, "0.") || strings.HasPrefix(postgisVersion, "1.") {
		err = populateGeometryColumn(tx, table.FullName, *table.Source)
		if err != nil {
			return errors.Wrap(err, "populating GeometryColumn for PostGIS < 2")
		}
	}

	err = tx.Commit()
	if err != nil {
		return errors.Wrapf(err, "commiting tx for generalizes table %q", table.FullName)
	}
	tx = nil // set nil to prevent rollback
	return nil
}

// Optimize clusters tables on new GeoHash index.
func (pg *PostGIS) Optimize() error {
	defer log.Step("Clustering on geometry")()

	worker := int(runtime.GOMAXPROCS(0))
	if worker < 1 {
		worker = 1
	}

	p := newWorkerPool(worker, len(pg.Tables)+len(pg.GeneralizedTables))

	for _, tbl := range pg.Tables {
		tableName := tbl.FullName
		table := tbl
		p.in <- func() error {
			return clusterTable(pg, tableName, table.Srid, table.Columns)
		}
	}
	for _, tbl := range pg.GeneralizedTables {
		tableName := tbl.FullName
		table := tbl
		p.in <- func() error {
			return clusterTable(pg, tableName, table.Source.Srid, table.Source.Columns)
		}
	}

	err := p.wait()
	if err != nil {
		return errors.Wrap(err, "optimizing database")
	}

	return nil
}

func clusterTable(pg *PostGIS, tableName string, srid int, columns []ColumnSpec) error {
	for _, col := range columns {
		if col.Type.Name() == "GEOMETRY" {
			step := log.Step(fmt.Sprintf("Indexing %q on geohash", tableName))
			sql := fmt.Sprintf(`CREATE INDEX "%s_geom_geohash" ON "%s"."%s" (ST_GeoHash(ST_Transform(ST_SetSRID(Box2D(%s), %d), 4326)))`,
				tableName, pg.Config.ImportSchema, tableName, col.Name, srid)
			_, err := pg.Db.Exec(sql)
			step()
			if err != nil {
				return errors.Wrapf(err, "indexing %q on geohash", tableName)
			}

			step = log.Step(fmt.Sprintf("Clustering %q on geohash", tableName))
			sql = fmt.Sprintf(`CLUSTER "%s_geom_geohash" ON "%s"."%s"`,
				tableName, pg.Config.ImportSchema, tableName)
			_, err = pg.Db.Exec(sql)
			step()
			if err != nil {
				return errors.Wrapf(err, "clusering %q on geohash", tableName)
			}
			break
		}
	}

	step := log.Step(fmt.Sprintf("Analysing %q", tableName))
	sql := fmt.Sprintf(`ANALYSE "%s"."%s"`,
		pg.Config.ImportSchema, tableName)
	_, err := pg.Db.Exec(sql)
	step()
	if err != nil {
		return errors.Wrapf(err, "analyzing %q", tableName)
	}

	return nil
}

type PostGIS struct {
	Db                      *sql.DB
	Params                  string
	Config                  database.Config
	Tables                  map[string]*TableSpec
	GeneralizedTables       map[string]*GeneralizedTableSpec
	Prefix                  string
	txRouter                *TxRouter
	updateGeneralizedTables bool

	updateIDsMu sync.Mutex
	updatedIDs  map[string][]int64
}

func (pg *PostGIS) Open() error {
	var err error

	pg.Db, err = sql.Open("postgres", pg.Params)
	if err != nil {
		return errors.Wrap(err, "opening Postgres DB")
	}
	// check that the connection actually works
	err = pg.Db.Ping()
	if err != nil {
		return errors.Wrap(err, "ping Postgres DB")
	}
	return nil
}

func (pg *PostGIS) InsertPoint(elem osm.Element, geom geom.Geometry, matches []mapping.Match) error {
	for _, match := range matches {
		row := match.Row(&elem, &geom)
		if err := pg.txRouter.Insert(match.Table.Name, row); err != nil {
			return err
		}
	}
	return nil
}

func (pg *PostGIS) InsertLineString(elem osm.Element, geom geom.Geometry, matches []mapping.Match) error {
	for _, match := range matches {
		row := match.Row(&elem, &geom)
		if err := pg.txRouter.Insert(match.Table.Name, row); err != nil {
			return err
		}
	}
	if pg.updateGeneralizedTables {
		genMatches := pg.generalizedFromMatches(matches)
		if len(genMatches) > 0 {
			pg.updateIDsMu.Lock()
			for _, generalizedTable := range genMatches {
				pg.updatedIDs[generalizedTable.Name] = append(pg.updatedIDs[generalizedTable.Name], elem.ID)

			}
			pg.updateIDsMu.Unlock()
		}
	}
	return nil
}

func (pg *PostGIS) InsertPolygon(elem osm.Element, geom geom.Geometry, matches []mapping.Match) error {
	for _, match := range matches {
		row := match.Row(&elem, &geom)
		if err := pg.txRouter.Insert(match.Table.Name, row); err != nil {
			return err
		}
	}
	if pg.updateGeneralizedTables {
		genMatches := pg.generalizedFromMatches(matches)
		if len(genMatches) > 0 {
			pg.updateIDsMu.Lock()
			for _, generalizedTable := range genMatches {
				pg.updatedIDs[generalizedTable.Name] = append(pg.updatedIDs[generalizedTable.Name], elem.ID)

			}
			pg.updateIDsMu.Unlock()
		}
	}
	return nil
}

func (pg *PostGIS) InsertRelationMember(rel osm.Relation, m osm.Member, mi int, geom geom.Geometry, matches []mapping.Match) error {
	for _, match := range matches {
		row := match.MemberRow(&rel, &m, mi, &geom)
		if err := pg.txRouter.Insert(match.Table.Name, row); err != nil {
			return err
		}
	}
	return nil
}

func (pg *PostGIS) Delete(id int64, matches []mapping.Match) error {
	for _, match := range matches {
		if err := pg.txRouter.Delete(match.Table.Name, id); err != nil {
			return errors.Wrapf(err, "deleting %d from %q", id, match.Table.Name)
		}
	}
	if pg.updateGeneralizedTables {
		for _, generalizedTable := range pg.generalizedFromMatches(matches) {
			if err := pg.txRouter.Delete(generalizedTable.Name, id); err != nil {
				return errors.Wrapf(err, "deleting %d from %q", id, generalizedTable.Name)
			}
		}
	}
	return nil
}

func (pg *PostGIS) generalizedFromMatches(matches []mapping.Match) []*GeneralizedTableSpec {
	generalizedTables := []*GeneralizedTableSpec{}
	for _, match := range matches {
		tbl := pg.Tables[match.Table.Name]
		generalizedTables = append(generalizedTables, tbl.Generalizations...)
	}
	return generalizedTables
}

func (pg *PostGIS) sortedGeneralizedTables() []string {
	added := map[string]bool{}
	sorted := []string{}

	for len(pg.GeneralizedTables) > len(sorted) {
		for _, tbl := range pg.GeneralizedTables {
			if _, ok := added[tbl.Name]; !ok {
				if tbl.Source != nil || added[tbl.SourceGeneralized.Name] {
					added[tbl.Name] = true
					sorted = append(sorted, tbl.Name)
				}
			}
		}
	}
	return sorted
}

func (pg *PostGIS) EnableGeneralizeUpdates() {
	pg.updateGeneralizedTables = true
	pg.updatedIDs = make(map[string][]int64)
}

func (pg *PostGIS) Begin() error {
	var err error
	pg.txRouter, err = newTxRouter(pg, false)
	return err
}

func (pg *PostGIS) BeginBulk() error {
	var err error
	pg.txRouter, err = newTxRouter(pg, true)
	return err
}

func (pg *PostGIS) Abort() error {
	return pg.txRouter.Abort()
}

func (pg *PostGIS) End() error {
	return pg.txRouter.End()
}

func (pg *PostGIS) Close() error {
	return pg.Db.Close()
}

func New(conf database.Config, m *config.Mapping) (database.DB, error) {
	db := &PostGIS{}

	db.Tables = make(map[string]*TableSpec)
	db.GeneralizedTables = make(map[string]*GeneralizedTableSpec)

	db.Config = conf

	connStr := db.Config.ConnectionParams

	// we accept postgis as an alias, replace for pq.ParseURL
	if strings.HasPrefix(connStr, "postgis:") {
		connStr = strings.Replace(
			connStr,
			"postgis", "postgres", 1,
		)
	}

	var err error
	var params string
	if strings.HasPrefix(connStr, "postgres://") {
		// connStr is a URL
		params, err = pq.ParseURL(connStr)
		if err != nil {
			return nil, errors.Wrap(err, "parsing database connection URL")
		}
	} else {
		// connStr is already a params list (postgres: host=localhost ...)
		params = strings.TrimSpace(strings.TrimPrefix(connStr, "postgres:"))
	}

	params = disableDefaultSsl(params)
	params, db.Prefix = stripPrefixFromConnectionParams(params)

	for name, table := range m.Tables {
		db.Tables[name], err = NewTableSpec(db, table)
		if err != nil {
			return nil, errors.Wrapf(err, "creating table spec for %q", name)
		}
	}
	for name, table := range m.GeneralizedTables {
		db.GeneralizedTables[name] = NewGeneralizedTableSpec(db, table)
	}
	if err := db.prepareGeneralizedTableSources(); err != nil {
		return nil, errors.Wrap(err, "preparing generalized table sources")
	}
	db.prepareGeneralizations()

	db.Params = params
	err = db.Open()
	if err != nil {
		return nil, errors.Wrap(err, "opening db")
	}
	return db, nil
}

// prepareGeneralizedTableSources checks if all generalized table have an
// existing source and sets .Source to the original source (works even
// when source is allready generalized).
func (pg *PostGIS) prepareGeneralizedTableSources() error {
	for name, table := range pg.GeneralizedTables {
		if source, ok := pg.Tables[table.SourceName]; ok {
			table.Source = source
		} else if source, ok := pg.GeneralizedTables[table.SourceName]; ok {
			table.SourceGeneralized = source
		} else {
			return errors.Errorf("missing source %q for generalized table %q",
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
	return nil
}

func (pg *PostGIS) prepareGeneralizations() {
	for _, table := range pg.GeneralizedTables {
		table.Source.Generalizations = append(table.Source.Generalizations, table)
		if source, ok := pg.GeneralizedTables[table.SourceName]; ok {
			source.Generalizations = append(source.Generalizations, table)
		}
	}
}

func init() {
	database.Register("postgres", New)
	database.Register("postgis", New)
}
