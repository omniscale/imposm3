package postgis

import (
	"fmt"
)

type ColumnType interface {
	Name() string
	PrepareInsertSql(i int,
		spec *TableSpec) string
	GeneralizeSql(colSpec *ColumnSpec, spec *GeneralizedTableSpec) string
}

type simpleColumnType struct {
	name string
}

func (t *simpleColumnType) Name() string {
	return t.name
}

func (t *simpleColumnType) PrepareInsertSql(i int, spec *TableSpec) string {
	return fmt.Sprintf("$%d", i)
}

func (t *simpleColumnType) GeneralizeSql(colSpec *ColumnSpec, spec *GeneralizedTableSpec) string {
	return colSpec.Name
}

type geometryType struct {
	name string
}

func (t *geometryType) Name() string {
	return t.name
}

func (t *geometryType) PrepareInsertSql(i int, spec *TableSpec) string {
	return fmt.Sprintf("ST_GeomFromWKB($%d, %d)",
		i, spec.Srid,
	)
}

func (t *geometryType) GeneralizeSql(colSpec *ColumnSpec, spec *GeneralizedTableSpec) string {
	return fmt.Sprintf(`ST_SimplifyPreserveTopology("%s", %f) as "%s"`,
		colSpec.Name, spec.Tolerance, colSpec.Name,
	)
}

var pgTypes map[string]ColumnType

func init() {
	pgTypes = map[string]ColumnType{
		"id":            &simpleColumnType{"BIGINT"},
		"geometry":      &geometryType{"GEOMETRY"},
		"bool":          &simpleColumnType{"BOOL"},
		"boolint":       &simpleColumnType{"SMALLINT"},
		"string":        &simpleColumnType{"VARCHAR"},
		"name":          &simpleColumnType{"VARCHAR"},
		"direction":     &simpleColumnType{"SMALLINT"},
		"integer":       &simpleColumnType{"INTEGER"},
		"wayzorder":     &simpleColumnType{"INTEGER"},
		"zorder":        &simpleColumnType{"INTEGER"},
		"pseudoarea":    &simpleColumnType{"REAL"},
		"mapping_key":   &simpleColumnType{"VARCHAR"},
		"mapping_value": &simpleColumnType{"VARCHAR"},
	}
}
