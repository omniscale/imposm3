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
		cols = append(cols, col.Name)
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

func NewTableSpec(conf *database.Config, t *mapping.Table, schema string) *TableSpec {
	spec := TableSpec{
		Name:         t.Name,
		Schema:       schema,
		GeometryType: t.Type,
		Srid:         conf.Srid,
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
	sql = fmt.Sprintf("SELECT AddGeometryColumn('%s', '%s', 'geometry', %d, '%s', 2);",
		spec.Schema, spec.Name, spec.Srid, strings.ToUpper(spec.GeometryType))
	row := pg.Db.QueryRow(sql)
	var void interface{}
	err = row.Scan(&void)
	if err != nil {
		return &SQLError{sql, err}
	}
	return nil
}

func (pg *PostGIS) createSchema() error {
	var sql string
	var err error

	if pg.Schema == "public" {
		return nil
	}

	sql = fmt.Sprintf("SELECT EXISTS(SELECT schema_name FROM information_schema.schemata WHERE schema_name = '%s');",
		pg.Schema)
	row := pg.Db.QueryRow(sql)
	var exists bool
	err = row.Scan(&exists)
	if err != nil {
		return &SQLError{sql, err}
	}
	if exists {
		return nil
	}

	sql = fmt.Sprintf("CREATE SCHEMA \"%s\"", pg.Schema)
	_, err = pg.Db.Exec(sql)
	if err != nil {
		return &SQLError{sql, err}
	}
	return nil
}

type PostGIS struct {
	Db     *sql.DB
	Schema string
	Config database.Config
	Tables map[string]*TableSpec
}

func schemaFromConnectionParams(params string) string {
	parts := strings.Fields(params)
	for _, p := range parts {
		if strings.HasPrefix(p, "schema=") {
			return strings.Replace(p, "schema=", "", 1)
		}
	}
	return "public"
}

func (pg *PostGIS) Open() error {
	var err error

	if strings.HasPrefix(pg.Config.ConnectionParams, "postgis://") {
		pg.Config.ConnectionParams = strings.Replace(
			pg.Config.ConnectionParams,
			"postgis", "postgres", 1,
		)
	}

	params, err := pq.ParseURL(pg.Config.ConnectionParams)
	pg.Schema = schemaFromConnectionParams(params)

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

func (pg *PostGIS) Init(m *mapping.Mapping) error {
	if err := pg.createSchema(); err != nil {
		return err
	}

	for name, table := range m.Tables {
		pg.Tables[name] = NewTableSpec(&pg.Config, table, pg.Schema)
	}
	for _, spec := range pg.Tables {
		if err := pg.createTable(*spec); err != nil {
			return err
		}
	}
	return nil
}

func New(conf database.Config) (database.DB, error) {
	db := &PostGIS{}
	db.Tables = make(map[string]*TableSpec)
	db.Config = conf
	err := db.Open()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func init() {
	database.Register("postgres", New)
	database.Register("postgis", New)
}
