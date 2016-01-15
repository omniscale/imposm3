package postgis

import (
	"fmt"
	"strings"

	"github.com/omniscale/imposm3/mapping"
)

type ColumnSpec struct {
	Name      string
	FieldType mapping.FieldType
	Type      ColumnType
}
type TableSpec struct {
	Name            string
	FullName        string
	Schema          string
	Columns         []ColumnSpec
	GeometryType    string
	Srid            int
	Generalizations []*GeneralizedTableSpec
}

type GeneralizedTableSpec struct {
	Name              string
	FullName          string
	Schema            string
	SourceName        string
	Source            *TableSpec
	SourceGeneralized *GeneralizedTableSpec
	Tolerance         float64
	Where             string
	created           bool
	Generalizations   []*GeneralizedTableSpec
}

func (col *ColumnSpec) AsSQL() string {
	return fmt.Sprintf("\"%s\" %s", col.Name, col.Type.Name())
}

func (spec *TableSpec) CreateTableSQL() string {
	foundIdCol := false
	for _, cs := range spec.Columns {
		if cs.Name == "id" {
			foundIdCol = true
		}
	}

	cols := []string{}
	if !foundIdCol {
		// only add id column if there is no id configured
		// TODO allow to disable id column?
		cols = append(cols, "id SERIAL PRIMARY KEY")
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
		spec.FullName,
		columns,
		placeholders,
	)
}

func (spec *TableSpec) CopySQL() string {
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

func (spec *TableSpec) DeleteSQL() string {
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

func NewTableSpec(pg *PostGIS, t *mapping.Table) *TableSpec {
	var geomType string
	switch t.Type {
	case mapping.RelationMemberTable:
		geomType = "geometry"
	default:
		geomType = string(t.Type)
	}

	spec := TableSpec{
		Name:         t.Name,
		FullName:     pg.Prefix + t.Name,
		Schema:       pg.Config.ImportSchema,
		GeometryType: geomType,
		Srid:         pg.Config.Srid,
	}
	for _, field := range t.Fields {
		fieldType := field.FieldType()
		if fieldType == nil {
			continue
		}
		pgType, ok := pgTypes[fieldType.GoType]
		if !ok {
			log.Errorf("unhandled field type %v, using string type", fieldType)
			pgType = pgTypes["string"]
		}
		col := ColumnSpec{field.Name, *fieldType, pgType}
		spec.Columns = append(spec.Columns, col)
	}
	return &spec
}

func NewGeneralizedTableSpec(pg *PostGIS, t *mapping.GeneralizedTable) *GeneralizedTableSpec {
	spec := GeneralizedTableSpec{
		Name:       t.Name,
		FullName:   pg.Prefix + t.Name,
		Schema:     pg.Config.ImportSchema,
		Tolerance:  t.Tolerance,
		Where:      t.SqlFilter,
		SourceName: t.SourceTableName,
	}
	return &spec
}

func (spec *GeneralizedTableSpec) DeleteSQL() string {
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

func (spec *GeneralizedTableSpec) InsertSQL() string {
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
		cols = append(cols, col.Type.GeneralizeSql(&col, spec))
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
