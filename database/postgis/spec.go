package postgis

import (
	"fmt"
	"strings"

	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/mapping/config"
	"github.com/pkg/errors"
)

type ColumnSpec struct {
	Name      string
	FieldType mapping.ColumnType
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
	foundIDCol := false
	pkCols := []string{}
	for _, cs := range spec.Columns {
		if cs.Name == "id" {
			foundIDCol = true
		}
		if cs.FieldType.Name == "id" {
			pkCols = append(pkCols, cs.Name)
		}
	}

	cols := []string{}
	if !foundIDCol {
		// Create explicit id column only if there is no id configured.
		cols = append(cols, "id BIGSERIAL")
		pkCols = append(pkCols, "id")
	}

	for _, col := range spec.Columns {
		if col.Type.Name() == "GEOMETRY" {
			continue
		}
		cols = append(cols, col.AsSQL())
	}

	// Make composite PRIMARY KEY of serial `id` and OSM ID. But only if the
	// user did not provide a custom `id` colum which might not be unique.
	if pkCols != nil && !foundIDCol {
		cols = append(cols, `PRIMARY KEY ("`+strings.Join(pkCols, `", "`)+`")`)
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
			col.Type.PrepareInsertSQL(len(vars)+1, spec))
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

func NewTableSpec(pg *PostGIS, t *config.Table) (*TableSpec, error) {
	var geomType string
	if mapping.TableType(t.Type) == mapping.RelationMemberTable {
		geomType = "geometry"
	} else {
		geomType = string(t.Type)
	}

	spec := TableSpec{
		Name:         t.Name,
		FullName:     pg.Prefix + t.Name,
		Schema:       pg.Config.ImportSchema,
		GeometryType: geomType,
		Srid:         pg.Config.Srid,
	}
	for _, column := range t.Columns {
		columnType, err := mapping.MakeColumnType(column)
		if err != nil {
			return nil, err
		}
		pgType, ok := pgTypes[columnType.GoType]
		if !ok {
			return nil, errors.Errorf("unhandled column type %v, using string type", columnType)
		}
		col := ColumnSpec{column.Name, *columnType, pgType}
		spec.Columns = append(spec.Columns, col)
	}
	return &spec, nil
}

func NewGeneralizedTableSpec(pg *PostGIS, t *config.GeneralizedTable) *GeneralizedTableSpec {
	spec := GeneralizedTableSpec{
		Name:       t.Name,
		FullName:   pg.Prefix + t.Name,
		Schema:     pg.Config.ImportSchema,
		Tolerance:  t.Tolerance,
		Where:      t.SQLFilter,
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
		cols = append(cols, col.Type.GeneralizeSQL(&col, spec))
	}

	where := fmt.Sprintf(` WHERE "%s" = $1`, idColumnName)
	if spec.Where != "" {
		where += " AND (" + spec.Where + ")"
	}

	var sourceTable string
	if spec.SourceGeneralized != nil {
		sourceTable = spec.SourceGeneralized.FullName
	} else {
		sourceTable = spec.Source.FullName
	}
	columnSQL := strings.Join(cols, ",\n")
	sql := fmt.Sprintf(`INSERT INTO "%s"."%s" (SELECT %s FROM "%s"."%s"%s)`,
		spec.Schema, spec.FullName, columnSQL, spec.Source.Schema,
		sourceTable, where)
	return sql

}
