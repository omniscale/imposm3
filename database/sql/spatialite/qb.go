package spatialite

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

func multiGeometryType(geomType string) string {
	// SQLITE is very strict about single vs. multi types
	// as those items are mixed in osm files only multi items are loaded
	// (and single items are casted to multi items)
	geomType = strings.ToUpper(geomType)

	if geomType == "POLYGON" {
		geomType = "MULTIPOLYGON" // for multipolygon support
	}

	if geomType == "LINESTRING" {
		geomType = "MULTILINESTRING" // for multipolygon support
	}

	if geomType == "POINT" {
		geomType = "MULTIPOINT" // for multipolygon support
	}

	return geomType
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
	return fmt.Sprintf(`SELECT EXISTS(SELECT * FROM sqlite_master WHERE type='table' and name='%s')`,
		table)
}

func DropTableSQL(schema string, table string) string {
	return fmt.Sprintf("DROP TABLE %s", table)
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
        CREATE TABLE IF NOT EXISTS "%s" (
            %s
        );`,
		spec.FullName,
		columnSQL,
	)
}

func (spec *QTableSpec) AddGeometryColumnSQL() string {
	geomType := multiGeometryType(spec.GeometryType)
	return fmt.Sprintf("SELECT AddGeometryColumn('%s', 'geometry', %d, '%s', 2);",
		spec.FullName, spec.Srid, geomType)
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

	return fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s)`,
		spec.FullName,
		columns,
		placeholders,
	)
}

func (spec *QTableSpec) CopySQL() string {
	return ""
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

	return fmt.Sprintf(`DELETE FROM "%s" WHERE "%s" = $1`,
		spec.FullName,
		idColumnName,
	)
}

func (spec *QGeneralizedTableSpec) DeleteSQL() string {
	// FIXME @see explanation at
	// (spec *QGeneralizedTableSpec) InsertSQL()

	return ""
	/*
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

		return fmt.Sprintf(`DELETE FROM "%s" WHERE "%s" = $1`,
			spec.FullName,
			idColumnName,
		)
	*/
}

func (spec *QGeneralizedTableSpec) InsertSQL() string {
	// FIXME this is currently disabled as the statement can't be prepared
	// when the generalized table doesn't exist (this bug affects the postgis)
	// implementation as well, but it's hidden as the postgis version uses
	// bulkimport by default
	return ""

	/*
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
		sql := fmt.Sprintf(`INSERT INTO "%s" SELECT %s FROM "%s"%s`,
			spec.FullName, columnSQL, spec.Source.FullName, where)
		return sql
	*/
}

func (q *QQueryBuilder) TableExistsSQL(schema string, table string) string {
	return TableExistsSQL(schema, table)
}

func (q *QQueryBuilder) DropTableSQL(schema string, table string) string {
	return DropTableSQL(schema, table)
}

func (q *QQueryBuilder) SchemaExistsSQL(schema string) string {
	return ""
}

func (q *QQueryBuilder) CreateSchemaSQL(schema string) string {
	return ""
}

func (spec *QQueryBuilder) PopulateGeometryColumnSQL(schema string, table string, geomType string, srid int) string {
	geomType = multiGeometryType(geomType)
	return fmt.Sprintf("SELECT RecoverGeometryColumn('%s', 'geometry', %d, '%s', 2);",
		table, srid, geomType)
}

func (spec *QQueryBuilder) CreateIndexSQL(schema string, table string, column string) string {
	return fmt.Sprintf(`CREATE INDEX "%s_idx" ON "%s" ("%s")`, table, table, column)
}

func (spec *QQueryBuilder) GeometryIndexesSQL(schema string, table string) string {
	return fmt.Sprintf(`
    SELECT REPLACE(name, 'idx_%s_', '')
      FROM sqlite_master
     WHERE type = 'table'
       AND name like 'idx_%s_%%'
       AND REPLACE(name, 'idx_%s_', '') NOT LIKE '%%\_%%' ESCAPE '\'
       AND rootpage = 0
           ;`,
		table, table, table)
}

func (spec *QQueryBuilder) DropGeometryIndexSQL(schema string, table string, column string) string {
	return spec.DropTableSQL(schema, fmt.Sprintf("idx_%s_%s", table, column))
}

func (spec *QQueryBuilder) DisableGeometryIndexSQL(schema string, table string, column string) string {
	return fmt.Sprintf(`SELECT DisableSpatialIndex('%s', '%s');`, table, column)
}

func (spec *QQueryBuilder) CreateGeometryIndexSQL(schema string, table string, column string) string {
	return fmt.Sprintf(`SELECT CreateSpatialIndex('%s', '%s')`, table, column)
}

func (spec *QQueryBuilder) CreateGeneralizedTableSQL(targetSchema string, targetTable string,
	columnSQL string, sourceSchema string, sourceTable string, where string) string {
	return fmt.Sprintf(`CREATE TABLE "%s" AS SELECT %s FROM "%s"%s`,
		targetTable, columnSQL, sourceTable, where)
}

func (spec *QQueryBuilder) TruncateTableSQL(schema string, table string) string {
	return fmt.Sprintf(`DELETE FROM "%s"`, table)
}

func (spec *QQueryBuilder) GeometryColumnExistsSQL(schema string, table string) string {
	return fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM geometry_columns WHERE f_table_name = '%s');",
		table)
}

func (spec *QQueryBuilder) DropGeometryColumnSQL(schema string, table string) string {
	return fmt.Sprintf("SELECT DiscardGeometryColumn('%s', 'geometry');",
		table)
}

func (spec *QQueryBuilder) ChangeTableSchemaSQL(currSchema string, table string, newSchema string) string {
	return ""
}
