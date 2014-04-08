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

func NewTableQueryBuilder(spec *sql.TableSpec) *QTableSpec {
	return &QTableSpec{*spec}
}

func NewGenTableQueryBuilder(spec *sql.GeneralizedTableSpec) *QGeneralizedTableSpec {
	return &QGeneralizedTableSpec{*spec}
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

func (spec *QTableSpec) CopySQL() string {
	var cols []string
	for _, col := range spec.Columns {
		cols = append(cols, "\""+col.Name+"\"")
	}
	columns := strings.Join(cols, ", ")

	return fmt.Sprintf(`COPY "%s"."%s" (%s) FROM STDIN`,
		spec.Schema,
		spec.FullName,
		columns,
	)
}

func (spec *QTableSpec) DeleteSQL() string {
	var idColumnName string
	for _, col := range spec.Columns {
		if col.FieldType.Name == "id" {
			idColumnName = col.Name
			break
		}
	}

	if idColumnName == "" {
		panic("missing id column")
	}

	return fmt.Sprintf(`DELETE FROM "%s"."%s" WHERE "%s" = $1`,
		spec.Schema,
		spec.FullName,
		idColumnName,
	)
}

func (spec *QGeneralizedTableSpec) DeleteSQL() string {
	var idColumnName string
	for _, col := range spec.Source.Columns {
		if col.FieldType.Name == "id" {
			idColumnName = col.Name
			break
		}
	}

	if idColumnName == "" {
		panic("missing id column")
	}

	return fmt.Sprintf(`DELETE FROM "%s"."%s" WHERE "%s" = $1`,
		spec.Schema,
		spec.FullName,
		idColumnName,
	)
}

func (spec *QGeneralizedTableSpec) InsertSQL() string {
	var idColumnName string
	for _, col := range spec.Source.Columns {
		if col.FieldType.Name == "id" {
			idColumnName = col.Name
			break
		}
	}

	if idColumnName == "" {
		panic("missing id column")
	}

	var cols []string
	for _, col := range spec.Source.Columns {
		cols = append(cols, col.Type.GeneralizeSql(&col, spec.Tolerance))
	}

	where := fmt.Sprintf(` WHERE "%s" = $1`, idColumnName)
	if spec.Where != "" {
		where += " AND (" + spec.Where + ")"
	}

	columnSQL := strings.Join(cols, ",\n")
	sql := fmt.Sprintf(`INSERT INTO "%s"."%s" (SELECT %s FROM "%s"."%s"%s)`,
		spec.Schema, spec.FullName, columnSQL, spec.Source.Schema,
		spec.Source.FullName, where)
	return sql

}