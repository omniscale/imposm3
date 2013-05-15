package postgis

import (
	"fmt"
)

type ColumnType interface {
	Name() string
	PrepareInsertSql(i int,
		spec *TableSpec) string
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

var pgTypes map[string]ColumnType

func init() {
	pgTypes = map[string]ColumnType{
		"id":            &simpleColumnType{"BIGINT"},
		"geometry":      &geometryType{"GEOMETRY"},
		"bool":          &simpleColumnType{"BOOL"},
		"string":        &simpleColumnType{"VARCHAR"},
		"name":          &simpleColumnType{"VARCHAR"},
		"direction":     &simpleColumnType{"SMALLINT"},
		"integer":       &simpleColumnType{"INTEGER"},
		"wayzorder":     &simpleColumnType{"INTEGER"},
		"pseudoarea":    &simpleColumnType{"REAL"},
		"mapping_key":   &simpleColumnType{"VARCHAR"},
		"mapping_value": &simpleColumnType{"VARCHAR"},
	}
}
