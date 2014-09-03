package mapping

import (
	"testing"

	"github.com/omniscale/imposm3/element"
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

func TestZOrder(t *testing.T) {
	match := Match{}

	zOrder, err := MakeZOrder("z_order",
		AvailableFieldTypes["z_order"],
		Field{
			Name: "z_order",
			Key:  "",
			Type: "z_order",
			Args: map[string]interface{}{"key": "fips", "ranks": []interface{}{"AA", "CC", "FF", "ZZ"}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	elem := &element.OSMElem{}

	elem.Tags = element.Tags{} // missing
	if v := zOrder("", elem, match); v != 0 {
		t.Errorf(" -> %v", v)
	}
	elem.Tags = element.Tags{"fips": "ABCD"} // unknown
	if v := zOrder("", elem, match); v != 0 {
		t.Errorf(" -> %v", v)
	}
	elem.Tags = element.Tags{"fips": "AA"}
	if v := zOrder("", elem, match); v != 4 {
		t.Errorf(" -> %v", v)
	}
	elem.Tags = element.Tags{"fips": "CC"}
	if v := zOrder("", elem, match); v != 3 {
		t.Errorf(" -> %v", v)
	}
	elem.Tags = element.Tags{"fips": "ZZ"}
	if v := zOrder("", elem, match); v != 1 {
		t.Errorf(" -> %v", v)
	}
}

func TestMakeSuffixReplace(t *testing.T) {
	field := Field{
		Name: "name", Key: "name", Type: "string_suffixreplace",
		Args: map[string]interface{}{"suffixes": map[string]interface{}{"Straße": "Str.", "straße": "str."}}}
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

func assertEq(t *testing.T, a, b string) {
	if a != b {
		t.Errorf("'%v' != '%v'", a, b)
	}
}

func TestHstoreString(t *testing.T) {
	match := Match{}
	assertEq(t, HstoreString("", &element.OSMElem{Tags: element.Tags{"key": "value"}}, match).(string), `"key"=>"value"`)
	assertEq(t, HstoreString("", &element.OSMElem{Tags: element.Tags{`"key"`: `'"value"'`}}, match).(string), `"\"key\""=>"'\"value\"'"`)
	assertEq(t, HstoreString("", &element.OSMElem{Tags: element.Tags{`\`: `\\\\`}}, match).(string), `"\\"=>"\\\\\\\\"`)
	assertEq(t, HstoreString("", &element.OSMElem{Tags: element.Tags{"Ümlåütê=>": ""}}, match).(string), `"Ümlåütê=>"=>""`)
}
