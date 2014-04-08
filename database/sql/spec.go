package sql

import (
	"fmt"
	"imposm3/mapping"
	"strings"
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

func (spec *TableSpec) InsertSQL() string {
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

func NewTableSpec(sdb *SQLDB, t *mapping.Table) *TableSpec {
	spec := TableSpec{
		Name:         t.Name,
		FullName:     sdb.Prefix + t.Name,
		Schema:       sdb.Config.ImportSchema,
		GeometryType: string(t.Type),
		Srid:         sdb.Config.Srid,
	}
	for _, field := range t.Fields {
		fieldType := field.FieldType()
		if fieldType == nil {
			continue
		}
		sdbType, ok := sdbTypes[fieldType.GoType]
		if !ok {
			log.Errorf("unhandled field type %v, using string type", fieldType)
			sdbType = sdbTypes["string"]
		}
		col := ColumnSpec{field.Name, *fieldType, sdbType}
		spec.Columns = append(spec.Columns, col)
	}
	return &spec
}

func NewGeneralizedTableSpec(sdb *SQLDB, t *mapping.GeneralizedTable) *GeneralizedTableSpec {
	spec := GeneralizedTableSpec{
		Name:       t.Name,
		FullName:   sdb.Prefix + t.Name,
		Schema:     sdb.Config.ImportSchema,
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
