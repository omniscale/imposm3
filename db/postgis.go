package db

import (
	"database/sql"
	"fmt"
	_ "github.com/bmizerany/pq"
	"goposm/element"
	"log"
	"strings"
)

type Config struct {
	Type             string
	ConnectionParams string
	Srid             int
	Schema           string
}

type DB interface {
	InsertWays([]element.Way, TableSpec) error
	Init(specs []TableSpec) error
}

type ColumnSpec struct {
	Name  string
	Type  string
	Value func(string, map[string]string, interface{}) interface{}
}
type TableSpec struct {
	Name         string
	Schema       string
	Columns      []ColumnSpec
	GeometryType string
	Srid         int
}

func (col *ColumnSpec) AsSQL() string {
	return fmt.Sprintf("\"%s\" %s", col.Name, col.Type)
}

func (spec *TableSpec) CreateTableSQL() string {
	cols := []string{
		"id SERIAL PRIMARY KEY",
		"osm_id BIGINT",
	}
	for _, col := range spec.Columns {
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

func (spec *TableSpec) WayValues(way element.Way) []interface{} {
	values := make([]interface{}, 0, len(spec.Columns)+2)
	values = append(values, way.Id)
	values = append(values, way.Wkb)
	for _, col := range spec.Columns {
		v, ok := way.Tags[col.Name]
		if !ok {
			values = append(values, nil)
		} else {
			if col.Value != nil {
				values = append(values, col.Value(v, way.Tags, way))
			} else {
				values = append(values, v)
			}
		}
	}
	return values
}

func (spec *TableSpec) InsertSQL() string {
	cols := []string{"osm_id", "geometry"}
	vars := []string{
		"$1",
		fmt.Sprintf("ST_GeomFromWKB($2, %d)", spec.Srid),
	}
	for i, col := range spec.Columns {
		cols = append(cols, col.Name)
		vars = append(vars, fmt.Sprintf("$%d", i+3))
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
		spec.Schema, spec.Name, spec.Srid, spec.GeometryType)
	_, err = pg.Db.Query(sql)
	if err != nil {
		return &SQLError{sql, err}
	}
	return nil
}

func (pg *PostGIS) createSchema() error {
	var sql string
	var err error

	if pg.Config.Schema == "public" {
		return nil
	}

	sql = fmt.Sprintf("SELECT EXISTS(SELECT schema_name FROM information_schema.schemata WHERE schema_name = '%s');",
		pg.Config.Schema)
	row := pg.Db.QueryRow(sql)
	var exists bool
	err = row.Scan(&exists)
	if err != nil {
		return &SQLError{sql, err}
	}
	if exists {
		return nil
	}

	sql = fmt.Sprintf("CREATE SCHEMA \"%s\"", pg.Config.Schema)
	_, err = pg.Db.Exec(sql)
	if err != nil {
		return &SQLError{sql, err}
	}
	return nil
}

type PostGIS struct {
	Db     *sql.DB
	Config Config
}

func (pg *PostGIS) Open() error {
	var err error
	pg.Db, err = sql.Open("postgres", pg.Config.ConnectionParams)
	if err != nil {
		return err
	}
	// sql.Open is lazy, make a query to check that the
	// connection actually works
	row := pg.Db.QueryRow("SELECT 1;")
	var v string
	err = row.Scan(&v)
	if err != nil {
		return err
	}
	return nil
}

func (pg *PostGIS) WayInserter(spec TableSpec, ways chan []element.Way) error {
	for ws := range ways {
		err := pg.InsertWays(ws, spec)
		if err != nil {
			return err
		}
	}
	return nil
}

func (pg *PostGIS) InsertWays(ways []element.Way, spec TableSpec) error {
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

	for _, w := range ways {
		_, err := stmt.Exec(spec.WayValues(w)...)
		if err != nil {
			return &SQLInsertError{SQLError{sql, err}, spec.WayValues(w)}
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	tx = nil
	return nil
}

func (pg *PostGIS) Init(specs []TableSpec) error {
	if err := pg.createSchema(); err != nil {
		return err
	}
	for _, spec := range specs {
		if err := pg.createTable(spec); err != nil {
			return err
		}
	}
	return nil
}

func Open(conf Config) (DB, error) {
	if conf.Type != "postgres" {
		panic("unsupported database type: " + conf.Type)
	}
	db := &PostGIS{}
	db.Config = conf
	err := db.Open()
	if err != nil {
		return nil, err
	}
	return db, nil
}

// func InitDb() {
// 	rawDb, err := sql.Open("postgres", "user=olt host=localhost dbname=olt sslmode=disable")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer rawDb.Close()

// 	pg := PostGIS{rawDb, "public"}
// 	pg.createSchema()

// 	spec := TableSpec{
// 		"goposm_test",
// 		pg.Schema,
// 		[]ColumnSpec{
// 			{"name", "VARCHAR"},
// 			{"highway", "VARCHAR"},
// 		},
// 		"LINESTRING",
// 		3857,
// 	}
// 	err = pg.createTable(spec)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// }

// func InsertWays(ways chan []element.Way, wg *sync.WaitGroup) {
// 	wg.Add(1)
// 	defer wg.Done()

// 	rawDb, err := sql.Open("postgres", "user=olt host=localhost dbname=olt sslmode=disable")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer rawDb.Close()

// 	pg := PostGIS{rawDb, "public"}

// 	spec := TableSpec{
// 		"goposm_test",
// 		pg.Schema,
// 		[]ColumnSpec{
// 			{"name", "VARCHAR"},
// 			{"highway", "VARCHAR"},
// 		},
// 		"LINESTRING",
// 		3857,
// 	}

// 	for ws := range ways {
// 		err = pg.insertWays(ws, spec)
// 		if err != nil {
// 			log.Fatal(err)
// 		}
// 	}
// }

// func main() {
// 	wayChan := make(chan element.Way)
// 	wg := &sync.WaitGroup{}

// 	go InsertWays(wayChan, wg)

// 	ways := []element.Way{
// 		{OSMElem: element.OSMElem{1234, element.Tags{"name": "Foo"}}, Wkb: []byte{0x0, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x0}},
// 		// {OSMElem: element.OSMElem{6666, element.Tags{"name": "Baz", "type": "motorway"}}},
// 		// {OSMElem: element.OSMElem{9999, element.Tags{"name": "Bar", "type": "bar"}}},
// 	}
// 	for _, w := range ways {
// 		wayChan <- w
// 	}
// 	close(wayChan)
// 	wg.Wait()
// }
