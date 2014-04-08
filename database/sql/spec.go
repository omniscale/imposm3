package sql

import (
	"fmt"
	"imposm3/mapping"
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