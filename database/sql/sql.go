package sql

import (
	"database/sql"
	"fmt"
	"imposm3/database"
	"imposm3/element"
	"imposm3/logging"
	"imposm3/mapping"
	"strings"
	"sync/atomic"
)

var log = logging.NewLogger("SQL")

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

func createTable(tx *sql.Tx, spec TableSpec, qb QueryBuilder, qbn NormalTableQueryBuilder) error {
	var sql string
	var err error

	err = dropTableIfExists(tx, qb, spec.Schema, spec.FullName)
	if err != nil {
		return err
	}

	sql = qbn.CreateTableSQL()
	_, err = tx.Exec(sql)
	if err != nil {
		return &SQLError{sql, err}
	}

	err = addGeometryColumn(tx, qbn)
	if err != nil {
		return err
	}
	return nil
}

func addGeometryColumn(tx *sql.Tx, qb NormalTableQueryBuilder) error {
	sql := qb.AddGeometryColumnSQL()
	row := tx.QueryRow(sql)
	var void interface{}
	err := row.Scan(&void)
	if err != nil {
		return &SQLError{sql, err}
	}
	return nil
}

func populateGeometryColumn(tx *sql.Tx, qb QueryBuilder, tableName string, spec TableSpec) error {
	sql := qb.PopulateGeometryColumnSQL(spec.Schema, tableName, spec.GeometryType, spec.Srid)

	row := tx.QueryRow(sql)
	var void interface{}
	err := row.Scan(&void)
	if err != nil {
		return &SQLError{sql, err}
	}
	return nil
}

func (sdb *SQLDB) createSchema(schema string) error {
	var sql string
	var err error

	if schema == "public" {
		return nil
	}

	sql = sdb.QB.SchemaExistsSQL(schema)

	if sql == "" {
		return nil
	}

	row := sdb.Db.QueryRow(sql)
	var exists bool
	err = row.Scan(&exists)
	if err != nil {
		return &SQLError{sql, err}
	}
	if exists {
		return nil
	}

	sql = sdb.QB.CreateSchemaSQL(schema)
	if sql == "" {
		return nil
	}

	_, err = sdb.Db.Exec(sql)
	if err != nil {
		return &SQLError{sql, err}
	}
	return nil
}

// Init creates schema and tables, drops existing data.
func (sdb *SQLDB) Init() error {
	if err := sdb.createSchema(sdb.Config.ImportSchema); err != nil {
		return err
	}

	tx, err := sdb.Db.Begin()
	if err != nil {
		return err
	}
	defer rollbackIfTx(&tx)
	for name, qb := range sdb.NormalTableQueryBuilder {
		if err := createTable(tx, *sdb.Tables[name], sdb.QB, qb); err != nil {
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
func (sdb *SQLDB) Finish() error {
	defer log.StopStep(log.StartStep(fmt.Sprintf("Creating geometry indices")))

	p := NewWorkerPool(sdb.Worker, len(sdb.Tables)+len(sdb.GeneralizedTables))
	for _, tbl := range sdb.Tables {
		tableName := tbl.FullName
		table := tbl
		p.In <- func() error {
			return createIndex(sdb, sdb.QB, tableName, table.Columns)
		}
	}

	for _, tbl := range sdb.GeneralizedTables {
		tableName := tbl.FullName
		table := tbl
		p.In <- func() error {
			return createIndex(sdb, sdb.QB, tableName, table.Source.Columns)
		}
	}

	err := p.Wait()
	if err != nil {
		return err
	}

	return nil
}

func createIndex(sdb *SQLDB, qb QueryBuilder, tableName string, columns []ColumnSpec) error {
	for _, col := range columns {
		if col.Type.Name() == "GEOMETRY" {
			sql := qb.CreateGeometryIndexSQL(sdb.Config.ImportSchema, tableName, col.Name)
			step := log.StartStep(fmt.Sprintf("Creating geometry index on %s", tableName))
			_, err := sdb.Db.Exec(sql)
			log.StopStep(step)
			if err != nil {
				return err
			}
		}
		if col.FieldType.Name == "id" {
			sql := qb.CreateIndexSQL(sdb.Config.ImportSchema, tableName, col.Name)
			step := log.StartStep(fmt.Sprintf("Creating OSM id index on %s", tableName))
			_, err := sdb.Db.Exec(sql)
			log.StopStep(step)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (sdb *SQLDB) GeneralizeUpdates() error {
	defer log.StopStep(log.StartStep(fmt.Sprintf("Updating generalized tables")))
	for _, table := range sdb.sortedGeneralizedTables() {
		if ids, ok := sdb.updatedIds[table]; ok {
			for _, id := range ids {
				err := sdb.txRouter.Insert(table, []interface{}{id})

				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (sdb *SQLDB) Generalize() error {
	defer log.StopStep(log.StartStep(fmt.Sprintf("Creating generalized tables")))

	// generalized tables can depend on other generalized tables
	// create tables with non-generalized sources first
	p := NewWorkerPool(sdb.Worker, len(sdb.GeneralizedTables))
	for _, table := range sdb.GeneralizedTables {
		if table.SourceGeneralized == nil {
			tbl := table // for following closure
			p.In <- func() error {
				if err := sdb.generalizeTable(tbl); err != nil {
					return err
				}
				tbl.created = true
				return nil
			}
		}
	}
	err := p.Wait()
	if err != nil {
		return err
	}

	// next create tables with created generalized sources until
	// no new source is created
	created := int32(1)
	for created == 1 {
		created = 0

		p := NewWorkerPool(sdb.Worker, len(sdb.GeneralizedTables))
		for _, table := range sdb.GeneralizedTables {
			if !table.created && table.SourceGeneralized.created {
				tbl := table // for following closure
				p.In <- func() error {
					if err := sdb.generalizeTable(tbl); err != nil {
						return err
					}
					tbl.created = true
					atomic.StoreInt32(&created, 1)
					return nil
				}
			}
		}
		err := p.Wait()
		if err != nil {
			return err
		}
	}
	return nil
}

func (sdb *SQLDB) generalizeTable(table *GeneralizedTableSpec) error {
	defer log.StopStep(log.StartStep(fmt.Sprintf("Generalizing %s into %s",
		table.Source.FullName, table.FullName)))

	tx, err := sdb.Db.Begin()
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
		cols = append(cols, col.Type.GeneralizeSql(&col, table.Tolerance))
	}

	if err := dropTableIfExists(tx, sdb.QB, sdb.Config.ImportSchema, table.FullName); err != nil {
		return err
	}

	columnSQL := strings.Join(cols, ",\n")

	var sourceTable string
	if table.SourceGeneralized != nil {
		sourceTable = table.SourceGeneralized.FullName
	} else {
		sourceTable = table.Source.FullName
	}

	sql := sdb.QB.CreateGeneralizedTableSQL(sdb.Config.ImportSchema, table.FullName,
		columnSQL, sdb.Config.ImportSchema,
		sourceTable, where)

	_, err = tx.Exec(sql)
	if err != nil {
		return &SQLError{sql, err}
	}

	err = populateGeometryColumn(tx, sdb.QB, table.FullName, *table.Source)
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
func (sdb *SQLDB) Optimize() error {
	if sdb.Optimizer != nil {
		return sdb.Optimizer(sdb)
	}

	return nil
}

type QueryBuilder interface {
	TableExistsSQL(string, string) string
	DropTableSQL(string, string) string
	GeometryColumnExistsSQL(string, string) string
	DropGeometryColumnSQL(string, string) string
	SchemaExistsSQL(string) string
	CreateSchemaSQL(string) string
	PopulateGeometryColumnSQL(string, string, string, int) string
	CreateIndexSQL(string, string, string) string
	CreateGeometryIndexSQL(string, string, string) string
	GeometryIndexesSQL(string, string) string
	DisableGeometryIndexSQL(string, string, string) string
	DropGeometryIndexSQL(string, string, string) string
	CreateGeneralizedTableSQL(string, string, string, string, string, string) string
	TruncateTableSQL(string, string) string
	ChangeTableSchemaSQL(string, string, string) string
}

type TableQueryBuilder interface {
	InsertSQL() string
	DeleteSQL() string
}

type NormalTableQueryBuilder interface {
	TableQueryBuilder
	CreateTableSQL() string
	AddGeometryColumnSQL() string
	CopySQL() string
}

type GenTableQueryBuilder interface {
	TableQueryBuilder
}

type ColumnType interface {
	Name() string
	PrepareInsertSql(i int) string
	GeneralizeSql(colSpec *ColumnSpec, tolerance float64) string
}

type OptimizerFunc func(*SQLDB) error

type SQLDB struct {
	Db                      *sql.DB
	Params                  string
	Config                  database.Config
	Tables                  map[string]*TableSpec
	QB                      QueryBuilder
	NormalTableQueryBuilder map[string]NormalTableQueryBuilder
	GenTableQueryBuilder    map[string]GenTableQueryBuilder
	GeneralizedTables       map[string]*GeneralizedTableSpec
	Prefix                  string
	txRouter                *TxRouter
	PointTagMatcher         *mapping.TagMatcher
	LineStringTagMatcher    *mapping.TagMatcher
	PolygonTagMatcher       *mapping.TagMatcher
	updateGeneralizedTables bool
	updatedIds              map[string][]int64
	Worker                  int
	BulkSupported           bool
	SdbTypes                map[string]ColumnType
	Optimizer               OptimizerFunc
	DeploymentSupported     bool
}

func (sdb *SQLDB) InsertPoint(elem element.OSMElem, matches interface{}) error {
	if matches, ok := matches.([]mapping.Match); ok {
		for _, match := range matches {
			row := match.Row(&elem)
			err := sdb.txRouter.Insert(match.Table.Name, row)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (sdb *SQLDB) InsertLineString(elem element.OSMElem, matches interface{}) error {
	if matches, ok := matches.([]mapping.Match); ok {
		for _, match := range matches {
			row := match.Row(&elem)
			err := sdb.txRouter.Insert(match.Table.Name, row)

			if err != nil {
				return err
			}
		}
		if sdb.updateGeneralizedTables {
			for _, generalizedTable := range sdb.generalizedFromMatches(matches) {
				sdb.updatedIds[generalizedTable.Name] = append(sdb.updatedIds[generalizedTable.Name], elem.Id)
			}
		}
	}

	return nil
}

func (sdb *SQLDB) InsertPolygon(elem element.OSMElem, matches interface{}) error {
	if matches, ok := matches.([]mapping.Match); ok {
		for _, match := range matches {
			row := match.Row(&elem)
			err := sdb.txRouter.Insert(match.Table.Name, row)

			if err != nil {
				return err
			}
		}
		if sdb.updateGeneralizedTables {
			for _, generalizedTable := range sdb.generalizedFromMatches(matches) {
				sdb.updatedIds[generalizedTable.Name] = append(sdb.updatedIds[generalizedTable.Name], elem.Id)
			}
		}
	}

	return nil
}

func (sdb *SQLDB) ProbePoint(elem element.OSMElem) (bool, interface{}) {
	if matches := sdb.PointTagMatcher.Match(&elem.Tags); len(matches) > 0 {
		return true, matches
	}
	return false, nil
}

func (sdb *SQLDB) ProbeLineString(elem element.OSMElem) (bool, interface{}) {
	if matches := sdb.LineStringTagMatcher.Match(&elem.Tags); len(matches) > 0 {
		return true, matches
	}
	return false, nil
}

func (sdb *SQLDB) ProbePolygon(elem element.OSMElem) (bool, interface{}) {
	if matches := sdb.PolygonTagMatcher.Match(&elem.Tags); len(matches) > 0 {
		return true, matches
	}
	return false, nil
}

func (sdb *SQLDB) SelectRelationPolygons(tags element.Tags, members []element.Member) []element.Member {
	relMatches := sdb.PolygonTagMatcher.Match(&tags)
	result := []element.Member{}
	for _, m := range members {
		if m.Type != element.WAY {
			continue
		}
		memberMatches := sdb.PolygonTagMatcher.Match(&m.Way.Tags)
		if matchEquals(relMatches, memberMatches) {
			result = append(result, m)
		}
	}
	return result
}

func matchEquals(matchesA, matchesB []mapping.Match) bool {
	for _, matchA := range matchesA {
		for _, matchB := range matchesB {
			if matchA.Key == matchB.Key &&
				matchA.Value == matchB.Value &&
				matchA.Table == matchB.Table {
				return true
			}
		}
	}
	return false
}

func (sdb *SQLDB) Delete(id int64, matches interface{}) error {
	if matches, ok := matches.([]mapping.Match); ok {
		for _, match := range matches {
			err := sdb.txRouter.Delete(match.Table.Name, id)

			if err != nil {
				return err
			}
		}
		if sdb.updateGeneralizedTables {
			for _, generalizedTable := range sdb.generalizedFromMatches(matches) {
				err := sdb.txRouter.Delete(generalizedTable.Name, id)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (sdb *SQLDB) DeleteElem(elem element.OSMElem) error {
	// handle deletes of geometries that did not match in ProbeXxx.
	// we have to handle multipolygon relations that took the tags of the
	// main-member. those tags are not avail. during delete. just try to
	// delete from each polygon table.
	if v, ok := elem.Tags["type"]; ok && (v == "multipolygon" || v == "boundary") {
		for _, tableSpec := range sdb.Tables {
			if tableSpec.GeometryType != "polygon" {
				continue
			}
			err := sdb.txRouter.Delete(tableSpec.Name, elem.Id)
			if err != nil {
				return err
			}
			if sdb.updateGeneralizedTables {
				for _, genTable := range tableSpec.Generalizations {
					err := sdb.txRouter.Delete(genTable.Name, elem.Id)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (sdb *SQLDB) generalizedFromMatches(matches []mapping.Match) []*GeneralizedTableSpec {
	generalizedTables := []*GeneralizedTableSpec{}
	for _, match := range matches {
		tbl := sdb.Tables[match.Table.Name]
		generalizedTables = append(generalizedTables, tbl.Generalizations...)
	}
	return generalizedTables
}

func (sdb *SQLDB) sortedGeneralizedTables() []string {
	added := map[string]bool{}
	sorted := []string{}

	for len(sdb.GeneralizedTables) > len(sorted) {
		for _, tbl := range sdb.GeneralizedTables {
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

func (sdb *SQLDB) EnableGeneralizeUpdates() {
	sdb.updateGeneralizedTables = true
	sdb.updatedIds = make(map[string][]int64)
}

func (sdb *SQLDB) Begin() error {
	var err error
	sdb.txRouter, err = newTxRouter(sdb, false)
	return err
}

func (sdb *SQLDB) BeginBulk() error {
	var err error
	sdb.txRouter, err = newTxRouter(sdb, sdb.BulkSupported)
	return err
}

func (sdb *SQLDB) Abort() error {
	return sdb.txRouter.Abort()
}

func (sdb *SQLDB) End() error {
	return sdb.txRouter.End()
}

func (sdb *SQLDB) Close() error {
	return sdb.Db.Close()
}

// PrepareGeneralizedTableSources checks if all generalized table have an
// existing source and sets .Source to the original source (works even
// when source is allready generalized).
func (sdb *SQLDB) PrepareGeneralizedTableSources() {
	for name, table := range sdb.GeneralizedTables {
		if source, ok := sdb.Tables[table.SourceName]; ok {
			table.Source = source
		} else if source, ok := sdb.GeneralizedTables[table.SourceName]; ok {
			table.SourceGeneralized = source
		} else {
			log.Printf("missing source '%s' for generalized table '%s'\n",
				table.SourceName, name)
		}
	}

	// set source table until all generalized tables have a source
	for filled := true; filled; {
		filled = false
		for _, table := range sdb.GeneralizedTables {
			if table.Source == nil {
				if source, ok := sdb.GeneralizedTables[table.SourceName]; ok && source.Source != nil {
					table.Source = source.Source
				}
				filled = true
			}
		}
	}
}

func (sdb *SQLDB) PrepareGeneralizations() {
	for _, table := range sdb.GeneralizedTables {
		table.Source.Generalizations = append(table.Source.Generalizations, table)
		if source, ok := sdb.GeneralizedTables[table.SourceName]; ok {
			source.Generalizations = append(source.Generalizations, table)
		}
	}
}
