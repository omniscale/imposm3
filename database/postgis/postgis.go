package postgis

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/bmizerany/pq"
	"goposm/database"
	"goposm/mapping"
	"log"
	"strings"
)

type ColumnSpec struct {
	Name string
	Type ColumnType
}
type TableSpec struct {
	Name         string
	Schema       string
	Columns      []ColumnSpec
	GeometryType string
	Srid         int
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

func NewTableSpec(pg *PostGIS, t *mapping.Table) *TableSpec {
	spec := TableSpec{
		Name:         pg.Prefix + t.Name,
		Schema:       pg.Schema,
		GeometryType: t.Type,
		Srid:         pg.Config.Srid,
	}
	for _, field := range t.Fields {
		pgType, ok := pgTypes[field.Type]
		if !ok {
			log.Println("unhandled", field)
			pgType = pgTypes["string"]
		}
		col := ColumnSpec{field.Name, pgType}
		spec.Columns = append(spec.Columns, col)
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
	Db           *sql.DB
	Schema       string
	BackupSchema string
	Config       database.Config
	Tables       map[string]*TableSpec
	Prefix       string
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
	defer func() {
		if tx != nil {
			if err := tx.Rollback(); err != nil {
				log.Println("rollback failed", err)
			}
		}
	}()

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
	tx = nil
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

func tableExists(tx *sql.Tx, schema, table string) (bool, error) {
	var exists bool
	sql := fmt.Sprintf(`SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='%s' AND table_schema='%s')`,
		table, schema)
	row := tx.QueryRow(sql)
	err := row.Scan(&exists)
	// fmt.Println(exists, err, sql)
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
	if err := pg.createSchema(backup); err != nil {
		return err
	}

	tx, err := pg.Db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if tx != nil {
			if err := tx.Rollback(); err != nil {
				log.Println("rollback failed", err)
			}
		}
	}()

	for tableName, _ := range pg.Tables {
		tableName = pg.Prefix + tableName

		log.Printf("rotating %s from %s -> %s -> %s\n", tableName, source, dest, backup)

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
			log.Printf("skipping rotate of %s, table does not exists in %s", tableName, source)
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
	tx = nil
	return nil
}

func (pg *PostGIS) Deploy() error {
	return pg.rotate(pg.Schema, "public", pg.BackupSchema)
}

func (pg *PostGIS) RevertDeploy() error {
	return pg.rotate(pg.BackupSchema, "public", pg.Schema)
}

func (pg *PostGIS) RemoveBackup() error {
	tx, err := pg.Db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if tx != nil {
			if err := tx.Rollback(); err != nil {
				log.Println("rollback failed", err)
			}
		}
	}()

	backup := pg.BackupSchema

	for tableName, _ := range pg.Tables {
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
	tx = nil
	return nil
}

func New(conf database.Config, m *mapping.Mapping) (database.DB, error) {
	db := &PostGIS{}
	db.Tables = make(map[string]*TableSpec)
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
