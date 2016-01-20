package postgis

import (
	"testing"
)

func TestRegisterColumnType(t *testing.T) {

	ctype := "jsonx3_string"
	registerColumnType(ctype, &simpleColumnType{"JSONX3"})

	if _, ok := pgTypes[ctype]; !ok {
		t.Errorf("  missing type: -> %v", ctype)
	}

}

func ExampleRegisterColumnType() {
	// register new PostgreSQL type :
	registerColumnType("example_jsonb_string", &simpleColumnType{"JSONB"}) // only  >= PostgreSQL 9.4
	registerColumnType("example_bytea_escape", &simpleColumnType{"BYTEA"})
	registerColumnType("example_double_precision", &simpleColumnType{"DOUBLE PRECISION"})
	registerColumnType("example_numeric", &simpleColumnType{"NUMERIC"})
	registerColumnType("example_int_array", &simpleColumnType{"INT[]"})
	registerColumnType("example_string_array", &simpleColumnType{"TEXT[]"})
	registerColumnType("example_bool_array", &simpleColumnType{"BOOL[]"})
	registerColumnType("example_int8_array", &simpleColumnType{"SMALLINT[]"})
	registerColumnType("example_float32_array", &simpleColumnType{"REAL[]"})

}
