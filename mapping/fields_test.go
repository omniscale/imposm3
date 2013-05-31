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

func TestMakeSuffixReplace(t *testing.T) {
	field := Field{
		"name", "name", "string_suffixreplace",
		map[string]interface{}{"suffixes": map[string]string{"Straße": "Str.", "straße": "str."}}}
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
