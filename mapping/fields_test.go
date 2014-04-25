package mapping

import (
	"testing"
)

func TestBool(t *testing.T) {
	match := Match{}
	if false != Bool("", nil, match) {
		t.Fatal()
	}
	if false != Bool("false", nil, match) {
		t.Fatal()
	}
	if false != Bool("no", nil, match) {
		t.Fatal()
	}
	if false != Bool("0", nil, match) {
		t.Fatal()
	}

	if true != Bool("yes", nil, match) {
		t.Fatal()
	}
	if true != Bool("1", nil, match) {
		t.Fatal()
	}
	if true != Bool("true", nil, match) {
		t.Fatal()
	}

	// Bool defaults to true
	if true != Bool("other", nil, match) {
		t.Fatal()
	}

}

func TestInteger(t *testing.T) {
	match := Match{}
	if v := Integer("", nil, match); v != nil {
		t.Errorf(" -> %v", v)
	}
	if v := Integer("bar", nil, match); v != nil {
		t.Errorf("bar -> %v", v)
	}
	if v := Integer("1e6", nil, match); v != nil {
		t.Errorf("1e6 -> %v", v)
	}
	if v := Integer("0", nil, match); v.(int64) != 0 {
		t.Errorf("0 -> %v", v)
	}
	if v := Integer("123456", nil, match); v.(int64) != 123456 {
		t.Errorf("123456 -> %v", v)
	}
	if v := Integer("-123456", nil, match); v.(int64) != -123456 {
		t.Errorf("-123456 -> %v", v)
	}
	// >2^32, but <2^64, Integer type defaults to int32
	if v := Integer("1000000000000000000", nil, match); v != nil {
		t.Errorf("1000000000000000000 -> %v", v)
	}
	// >2^64
	if v := Integer("19082139812039812093908123", nil, match); v != nil {
		t.Errorf("19082139812039812093908123 -> %v", v)
	}

}

func TestMakeSuffixReplace(t *testing.T) {
	field := Field{
		"name", "name", "string_suffixreplace",
		map[string]interface{}{"suffixes": map[string]interface{}{"Straße": "Str.", "straße": "str."}}}
	suffixReplace, err := MakeSuffixReplace("name", FieldType{}, field)

	if err != nil {
		t.Fatal(err)
	}

	if result := suffixReplace("Hauptstraße", nil, Match{}); result != "Hauptstr." {
		t.Fatal(result)
	}
	if result := suffixReplace("", nil, Match{}); result != "" {
		t.Fatal(result)
	}
	if result := suffixReplace("Foostraßeee", nil, Match{}); result != "Foostraßeee" {
		t.Fatal(result)
	}
}
