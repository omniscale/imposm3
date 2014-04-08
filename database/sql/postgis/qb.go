package postgis

import (
  "imposm3/database/sql"
  "strings"
  "fmt"
)

type QColumnSpec struct {
  sql.ColumnSpec
}

type QTableSpec struct {
  sql.TableSpec
}

type QGeneralizedTableSpec struct {
  sql.GeneralizedTableSpec
}

func NewQueryBuilder(spec *sql.TableSpec) *QTableSpec {
	return &QTableSpec{*spec}
}

func (spec *QTableSpec) CreateTableSQL() string {
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
		spec.FullName,
		columnSQL,
	)
}

func (spec *QTableSpec) AddGeometryColumn() string {
	geomType := strings.ToUpper(spec.GeometryType)
  
	if geomType == "POLYGON" {
		geomType = "GEOMETRY" // for multipolygon support
	}
  
	return fmt.Sprintf("SELECT AddGeometryColumn('%s', '%s', 'geometry', '%d', '%s', 2);",
		spec.Schema, spec.FullName, spec.Srid, geomType)
}

func (spec *QTableSpec) InsertSQL() string {
	var cols []string
	var vars []string
	for _, col := range spec.Columns {
		cols = append(cols, "\""+col.Name+"\"")
		vars = append(vars,
			col.Type.PrepareInsertSql(len(vars)+1))
	}
	columns := strings.Join(cols, ", ")
	placeholders := strings.Join(vars, ", ")

	return fmt.Sprintf(`INSERT INTO "%s"."%s" (%s) VALUES (%s)`,
		spec.Schema,
		spec.FullName,
		columns,
		placeholders,
	)
}