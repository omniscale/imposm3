package db

type ColumnType struct {
	Name          string
	ValueTemplate string
}

var pgTypes map[string]ColumnType

func init() {
	pgTypes = map[string]ColumnType{
		"id":        {"BIGINT", ""},
		"geometry":  {"GEOMETRY", "ST_GeomFromWKB($%d, 3857)"},
		"bool":      {"BOOL", ""},
		"string":    {"VARCHAR", ""},
		"name":      {"VARCHAR", ""},
		"direction": {"SMALLINT", ""},
		"integer":   {"INTEGER", ""},
	}
}
