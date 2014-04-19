package spatialite

import (
	"fmt"
	"imposm3/database/sql"
)

type simpleColumnType struct {
	name string
}

func (t *simpleColumnType) Name() string {
	return t.name
}

func (t *simpleColumnType) PrepareInsertSql(i int) string {
	return fmt.Sprintf("$%d", i)
}

func (t *simpleColumnType) GeneralizeSql(colSpec *sql.ColumnSpec, tolerance float64) string {
	return "\"" + colSpec.Name + "\""
}

type geometryType struct {
	name string
}

func (t *geometryType) Name() string {
	return t.name
}

func (t *geometryType) PrepareInsertSql(i int) string {
	return fmt.Sprintf("CastToMulti(GeomFromEWKB($%d))", i)
}

func (t *geometryType) GeneralizeSql(colSpec *sql.ColumnSpec, tolerance float64) string {
	return fmt.Sprintf(`CastToMulti(ST_SimplifyPreserveTopology("%s", %f)) as "%s"`,
		colSpec.Name, tolerance, colSpec.Name,
	)
}

type validatedGeometryType struct {
	geometryType
}

func (t *validatedGeometryType) GeneralizeSql(colSpec *sql.ColumnSpec, tolerance float64) string {
	return fmt.Sprintf(`CastToMulti(ST_Buffer(ST_SimplifyPreserveTopology("%s", %f), 0)) as "%s"`,
		colSpec.Name, tolerance, colSpec.Name,
	)
}

func NewSdbTypes() map[string]sql.ColumnType {
	return map[string]sql.ColumnType{
		"string":             &simpleColumnType{"VARCHAR"},
		"bool":               &simpleColumnType{"BOOL"},
		"int8":               &simpleColumnType{"SMALLINT"},
		"int32":              &simpleColumnType{"INT"},
		"int64":              &simpleColumnType{"BIGINT"},
		"float32":            &simpleColumnType{"REAL"},
		"geometry":           &geometryType{"GEOMETRY"},
		"validated_geometry": &validatedGeometryType{geometryType{"GEOMETRY"}},
	}
}
