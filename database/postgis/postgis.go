package postgis

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/bmizerany/pq"
	"goposm/database"
	"goposm/logging"
	"goposm/mapping"
	"strings"
)

var log = logging.NewLogger("PostGIS")

type ColumnSpec struct {
	Name      string
	FieldType mapping.FieldType
	Type      ColumnType
}
type TableSpec struct {
	Name         string
	Schema       string
	Columns      []ColumnSpec
	GeometryType string
	Srid         int
}

type GeneralizedTableSpec struct {
	Name              string
	SourceName        string
	Source            *TableSpec
	SourceGeneralized *GeneralizedTableSpec
	Tolerance         float64
	Where             string
	created           bool
}

func (col *ColumnSpec) AsSQL() string {
	return fmt.Sprintf("\"%s\" %s", col.Name, col.Type.Name())
}

func (spec *TableSpec) CreateTableSQL() string {
	cols := []string{
		"id SERIAL PRIMARY KEY",
	}
	for _, col := range spec.Columns {
		if col.Type.Name() == "GEOMETRY" {
			continue
		}
		cols = append(cols, col.AsSQL())
	}
	columnSQL := strings.Join(cols, ",\n")
	return fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS "%s"."%s" (
            %s
        );`,
		spec.Schema,
		spec.Name,
		columnSQL,
	)
}

func (spec *TableSpec) InsertSQL() string {
	var cols []string
	var vars []string
	for _, col := range spec.Columns {
		cols = append(cols, "\""+col.Name+"\"")
		vars = append(vars,
			col.Type.PrepareInsertSql(len(vars)+1, spec))
	}
	columns := strings.Join(cols, ", ")
	placeholders := strings.Join(vars, ", ")

	return fmt.Sprintf(`INSERT INTO "%s"."%s" (%s) VALUES (%s)`,
		spec.Schema,
		spec.Name,
		columns,
		placeholders,
	)
}

func (spec *TableSpec) DeleteSQL() string {
	var idColumName string
	for _, col := range spec.Columns {
		if col.FieldType.Name == "id" {
			idColumName = col.Name
			break
		}
	}

	if idColumName == "" {
		panic("missing id column")
	}

	return fmt.Sprintf(`DELETE FROM "%s"."%s" WHERE "%s" = $1`,
		spec.Schema,
		spec.Name,
		idColumName,
	)
}

func NewTableSpec(pg *PostGIS, t *mapping.Table) *TableSpec {
	spec := TableSpec{
		Name:         pg.Prefix + t.Name,
		Schema:       pg.Schema,
		GeometryType: t.Type,
		Srid:         pg.Config.Srid,
	}
	for _, field := range t.Fields {
		fieldType := field.FieldType()
		if fieldType == nil {
			continue
		}
		pgType, ok := pgTypes[fieldType.GoType]
		if !ok {
			log.Errorf("unhandled field type %v, using string type", fieldType)
			pgType = pgTypes["string"]
		}
		col := ColumnSpec{field.Name, *fieldType, pgType}
		spec.Columns = append(spec.Columns, col)
	}
	return &spec
}

func NewGeneralizedTableSpec(pg *PostGIS, t *mapping.GeneralizedTable) *GeneralizedTableSpec {
	spec := GeneralizedTableSpec{
		Name:       pg.Prefix + t.Name,
		Tolerance:  t.Tolerance,
		Where:      t.SqlFilter,
		SourceName: t.SourceTableName,
	}
	return &spec
}

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

func (pg *PostGIS) createTable(spec TableSpec) error {
	var sql string
	var err error
	sql = fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s"`, spec.Schema, spec.Name)
	_, err = pg.Db.Exec(sql)
	if err != nil {
		return &SQLError{sql, err}
	}

	sql = spec.CreateTableSQL()
	_, err = pg.Db.Exec(sql)
	if err != nil {
		return &SQLError{sql, err}
	}
	geomType := strings.ToUpper(spec.GeometryType)
	if geomType == "POLYGON" {
		geomType = "GEOMETRY" // for multipolygon support
	}
	sql = fmt.Sprintf("SELECT AddGeometryColumn('%s', '%s', 'geometry', %d, '%s', 2);",
		spec.Schema, spec.Name, spec.Srid, geomType)
	row := pg.Db.QueryRow(sql)
	var void interface{}
	err = row.Scan(&void)
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

type PostGIS struct {
	Db                *sql.DB
	Schema            string
	BackupSchema      string
	Config            database.Config
	Tables            map[string]*TableSpec
	GeneralizedTables map[string]*GeneralizedTableSpec
	Prefix            string
}

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

func (pg *PostGIS) Open() error {
	var err error

	params, err := pq.ParseURL(pg.Config.ConnectionParams)
	if err != nil {
		return err
	}
	pg.Db, err = sql.Open("postgres", params)
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

func (pg *PostGIS) Delete(table string, id int64) error {
	spec, ok := pg.Tables[table]
	if !ok {
		return errors.New("unkown table: " + table)
	}

	tx, err := pg.Db.Begin()
	if err != nil {
		return err
	}
	defer rollbackIfTx(&tx)

	sql := spec.DeleteSQL()
	stmt, err := tx.Prepare(sql)
	if err != nil {
		return &SQLError{sql, err}
	}
	defer stmt.Close()

	_, err = stmt.Exec(id)
	if err != nil {
		return &SQLInsertError{SQLError{sql, err}, id}
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

	for _, spec := range pg.Tables {
		if err := pg.createTable(*spec); err != nil {
			return err
		}
	}
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
	sql := fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s"`, schema, table)
	_, err := tx.Exec(sql)
	return err
}

func (pg *PostGIS) rotate(source, dest, backup string) error {
	defer log.StopStep(log.StartStep(fmt.Sprintf("Rotating tables")))

	if err := pg.createSchema(backup); err != nil {
		return err
	}

	tx, err := pg.Db.Begin()
	if err != nil {
		return err
	}
	defer rollbackIfTx(&tx)

	for _, tableName := range pg.TableNames() {
		tableName = pg.Prefix + tableName

		log.Printf("Rotating %s from %s -> %s -> %s", tableName, source, dest, backup)

		backupExists, err := tableExists(tx, backup, tableName)
		if err != nil {
			return err
		}
		sourceExists, err := tableExists(tx, source, tableName)
		if err != nil {
			return err
		}
		destExists, err := tableExists(tx, dest, tableName)
		if err != nil {
			return err
		}

		if !sourceExists {
			log.Warnf("skipping rotate of %s, table does not exists in %s", tableName, source)
			continue
		}

		if destExists {
			log.Printf("backup of %s, to %s", tableName, backup)
			if backupExists {
				err = dropTableIfExists(tx, backup, tableName)
				if err != nil {
					return err
				}
			}
			sql := fmt.Sprintf(`ALTER TABLE "%s"."%s" SET SCHEMA "%s"`, dest, tableName, backup)
			_, err = tx.Exec(sql)
			if err != nil {
				return err
			}
		}

		sql := fmt.Sprintf(`ALTER TABLE "%s"."%s" SET SCHEMA "%s"`, source, tableName, dest)
		_, err = tx.Exec(sql)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	tx = nil // set nil to prevent rollback
	return nil
}

func (pg *PostGIS) Deploy() error {
	return pg.rotate(pg.Schema, "public", pg.BackupSchema)
}

func (pg *PostGIS) RevertDeploy() error {
	return pg.rotate(pg.BackupSchema, "public", pg.Schema)
}

func rollbackIfTx(tx **sql.Tx) {
	if *tx != nil {
		if err := tx.Rollback(); err != nil {
			log.Fatal("rollback failed", err)
		}
	}
}

func (pg *PostGIS) RemoveBackup() error {
	tx, err := pg.Db.Begin()
	if err != nil {
		return err
	}
	defer rollbackIfTx(&tx)

	backup := pg.BackupSchema

	for _, tableName := range pg.TableNames() {
		tableName = pg.Prefix + tableName

		backupExists, err := tableExists(tx, backup, tableName)
		if err != nil {
			return err
		}
		if backupExists {
			log.Printf("removing backup of %s from %s", tableName, backup)
			err = dropTableIfExists(tx, backup, tableName)
			if err != nil {
				return err
			}

		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	tx = nil // set nil to prevent rollback
	return nil
}

// Finish creates spatial indices on all tables.
func (pg *PostGIS) Finish() error {
	defer log.StopStep(log.StartStep(fmt.Sprintf("Creating geometry indices")))

	tx, err := pg.Db.Begin()
	if err != nil {
		return err
	}
	defer rollbackIfTx(&tx)

	for tableName, table := range pg.Tables {
		tableName := pg.Prefix + tableName
		for _, col := range table.Columns {
			if col.Type.Name() == "GEOMETRY" {
				sql := fmt.Sprintf(`CREATE INDEX "%s_geom" ON "%s"."%s" USING GIST ("%s")`,
					tableName, pg.Schema, tableName, col.Name)
				step := log.StartStep(fmt.Sprintf("Creating geometry index on %s", tableName))
				_, err := tx.Exec(sql)
				log.StopStep(step)
				if err != nil {
					return err
				}
			}
		}
	}
	for tableName, table := range pg.GeneralizedTables {
		tableName := pg.Prefix + tableName
		for _, col := range table.Source.Columns {
			if col.Type.Name() == "GEOMETRY" {
				sql := fmt.Sprintf(`CREATE INDEX "%s_geom" ON "%s"."%s" USING GIST ("%s")`,
					tableName, pg.Schema, tableName, col.Name)
				step := log.StartStep(fmt.Sprintf("Creating geometry index on %s", tableName))
				_, err := tx.Exec(sql)
				log.StopStep(step)
				if err != nil {
					return err
				}
			}
		}
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	tx = nil // set nil to prevent rollback
	return nil
}

func (pg *PostGIS) checkGeneralizedTableSources() {
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

	filled := true
	for filled {
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

func (pg *PostGIS) Generalize() error {
	defer log.StopStep(log.StartStep(fmt.Sprintf("Creating generalized tables")))

	// generalized tables can depend on other generalized tables
	// create tables with non-generalized sources first
	for _, table := range pg.GeneralizedTables {
		if table.SourceGeneralized == nil {
			if err := pg.generalizeTable(table); err != nil {
				return err
			}
			table.created = true
		}
	}
	// next create tables with created generalized sources until
	// no new source is created
	created := true
	for created {
		created = false
		for _, table := range pg.GeneralizedTables {
			if !table.created && table.SourceGeneralized.created {
				if err := pg.generalizeTable(table); err != nil {
					return err
				}
				table.created = true
				created = true
			}
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
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	tx = nil // set nil to prevent rollback
	return nil
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
	db.Schema, db.BackupSchema = schemasFromConnectionParams(params)
	db.Prefix = prefixFromConnectionParams(params)

	for name, table := range m.Tables {
		db.Tables[name] = NewTableSpec(db, table)
	}
	for name, table := range m.GeneralizedTables {
		db.GeneralizedTables[name] = NewGeneralizedTableSpec(db, table)
	}
	db.checkGeneralizedTableSources()

	err = db.Open()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func init() {
	database.Register("postgres", New)
	database.Register("postgis", New)
}
