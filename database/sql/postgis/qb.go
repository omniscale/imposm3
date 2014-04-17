package postgis

import (
	"fmt"
	"imposm3/database/sql"
	"strings"
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

type QQueryBuilder struct {
}

func NewNormalTableQueryBuilder(spec *sql.TableSpec) *QTableSpec {
	return &QTableSpec{*spec}
}

func NewGenTableQueryBuilder(spec *sql.GeneralizedTableSpec) *QGeneralizedTableSpec {
	return &QGeneralizedTableSpec{*spec}
}

func NewQueryBuilder() *QQueryBuilder {
	return &QQueryBuilder{}
}

func TableExistsSQL(schema string, table string) string {
	return fmt.Sprintf(`SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='%s' AND table_schema='%s')`,
		table, schema)
}

func DropTableSQL(schema string, table string) string {
	return fmt.Sprintf("SELECT DropGeometryTable('%s', '%s');", schema, table)
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

func (spec *QTableSpec) AddGeometryColumnSQL() string {
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

func (q *QQueryBuilder) TableExistsSQL(schema string, table string) string {
	return TableExistsSQL(schema, table)
}

func (q *QQueryBuilder) DropTableSQL(schema string, table string) string {
	return DropTableSQL(schema, table)
}

func (q *QQueryBuilder) SchemaExistsSQL(schema string) string {
	return fmt.Sprintf("SELECT EXISTS(SELECT schema_name FROM information_schema.schemata WHERE schema_name = '%s');",
		schema)
}

func (q *QQueryBuilder) CreateSchemaSQL(schema string) string {
	return fmt.Sprintf("CREATE SCHEMA \"%s\"", schema)
}

func (spec *QQueryBuilder) PopulateGeometryColumnSQL(schema string, table string, geomType string, srid int) string {
	return fmt.Sprintf("SELECT Populate_Geometry_Columns('%s.%s'::regclass);",
		schema, table)
}

func (spec *QQueryBuilder) CreateIndexSQL(schema string, table string, column string) string {
  return fmt.Sprintf(`CREATE INDEX "%s_osm_id_idx" ON "%s"."%s" USING BTREE ("%s")`,
  				table, schema, table, column)
}

func (spec *QQueryBuilder) CreateGeometryIndexSQL(schema string, table string, column string) string {
  return fmt.Sprintf(`CREATE INDEX "%s_geom" ON "%s"."%s" USING GIST ("%s")`,
  				table, schema, table, column) 
}

func (spec *QQueryBuilder) CreateGeneralizedTableSQL(targetSchema string, targetTable string,
  columnSQL string, sourceSchema string, sourceTable string, where string) string {
	return fmt.Sprintf(`CREATE TABLE "%s"."%s" AS (SELECT %s FROM "%s"."%s"%s)`,
    targetSchema, targetTable, columnSQL, sourceSchema, sourceTable, where)
}

func (spec *QQueryBuilder) TruncateTableSQL(schema string, table string) string {
  return fmt.Sprintf(`TRUNCATE TABLE "%s"."%s" RESTART IDENTITY`, schema, table)
}

