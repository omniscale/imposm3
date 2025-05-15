package mapping

import (
	"testing"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/geom/geos"
	"github.com/omniscale/imposm3/mapping/config"
)

func TestBool(t *testing.T) {
	match := Match{}
	if false != Bool("", nil, nil, match) {
		t.Fatal()
	}
	if false != Bool("false", nil, nil, match) {
		t.Fatal()
	}
	if false != Bool("no", nil, nil, match) {
		t.Fatal()
	}
	if false != Bool("0", nil, nil, match) {
		t.Fatal()
	}

	if true != Bool("yes", nil, nil, match) {
		t.Fatal()
	}
	if true != Bool("1", nil, nil, match) {
		t.Fatal()
	}
	if true != Bool("true", nil, nil, match) {
		t.Fatal()
	}

	// Bool defaults to true
	if true != Bool("other", nil, nil, match) {
		t.Fatal()
	}

}

func TestInteger(t *testing.T) {
	match := Match{}
	if v := Integer("", nil, nil, match); v != nil {
		t.Errorf(" -> %v", v)
	}
	if v := Integer("bar", nil, nil, match); v != nil {
		t.Errorf("bar -> %v", v)
	}
	if v := Integer("1e6", nil, nil, match); v != nil {
		t.Errorf("1e6 -> %v", v)
	}
	if v := Integer("0", nil, nil, match); v.(int64) != 0 {
		t.Errorf("0 -> %v", v)
	}
	if v := Integer("123456", nil, nil, match); v.(int64) != 123456 {
		t.Errorf("123456 -> %v", v)
	}
	if v := Integer("-123456", nil, nil, match); v.(int64) != -123456 {
		t.Errorf("-123456 -> %v", v)
	}
	// >2^32, but <2^64, Integer type defaults to int32
	if v := Integer("1000000000000000000", nil, nil, match); v != nil {
		t.Errorf("1000000000000000000 -> %v", v)
	}
	// >2^64
	if v := Integer("19082139812039812093908123", nil, nil, match); v != nil {
		t.Errorf("19082139812039812093908123 -> %v", v)
	}
}

func TestZOrder(t *testing.T) {
	match := Match{}

	zOrder, err := MakeZOrder("z_order",
		AvailableColumnTypes["z_order"],
		config.Column{
			Name: "z_order",
			Key:  "",
			Type: "z_order",
			Args: map[string]interface{}{"key": "fips", "ranks": []interface{}{"AA", "CC", "FF", "ZZ"}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	elem := &osm.Element{}

	elem.Tags = osm.Tags{} // missing
	if v := zOrder("", elem, nil, match); v != 0 {
		t.Errorf(" -> %v", v)
	}
	elem.Tags = osm.Tags{"fips": "ABCD"} // unknown
	if v := zOrder("", elem, nil, match); v != 0 {
		t.Errorf(" -> %v", v)
	}
	elem.Tags = osm.Tags{"fips": "AA"}
	if v := zOrder("", elem, nil, match); v != 4 {
		t.Errorf(" -> %v", v)
	}
	elem.Tags = osm.Tags{"fips": "CC"}
	if v := zOrder("", elem, nil, match); v != 3 {
		t.Errorf(" -> %v", v)
	}
	elem.Tags = osm.Tags{"fips": "ZZ"}
	if v := zOrder("", elem, nil, match); v != 1 {
		t.Errorf(" -> %v", v)
	}
}

func TestEnumerate_Match(t *testing.T) {
	// test enumerate by matched mapping key

	zOrder, err := MakeEnumerate("enumerate",
		AvailableColumnTypes["enumerate"],
		config.Column{
			Name: "enumerate",
			Key:  "",
			Type: "enumerate",
			Args: map[string]interface{}{"values": []interface{}{"AA", "CC", "FF", "ZZ"}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		key      string
		tags     osm.Tags
		expected int
	}{
		{"", nil, 0},
		{"ABCD", nil, 0},
		{"AA", nil, 1},
		{"CC", nil, 2},
		{"ZZ", nil, 4},
	}
	for _, test := range tests {
		elem := &osm.Element{Tags: test.tags}
		match := Match{Value: test.key}
		if v := zOrder("", elem, nil, match); v.(int) != test.expected {
			t.Errorf("%v %v %d != %d", test.key, test.tags, v, test.expected)
		}
	}
}

func TestEnumerate_Key(t *testing.T) {
	// test enumerate by key

	zOrder, err := MakeEnumerate("enumerate",
		AvailableColumnTypes["enumerate"],
		config.Column{
			Name: "enumerate",
			Key:  "fips",
			Type: "enumerate",
			Args: map[string]interface{}{"values": []interface{}{"AA", "CC", "FF", "ZZ"}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		key      string
		tags     osm.Tags
		expected int
	}{
		{"", nil, 0},
		{"ABCD", nil, 0},
		{"AA", nil, 1},
		{"CC", nil, 2},
		{"ZZ", nil, 4},
	}
	for _, test := range tests {
		elem := &osm.Element{Tags: test.tags}
		match := Match{}
		if v := zOrder(test.key, elem, nil, match); v.(int) != test.expected {
			t.Errorf("%v %v %d != %d", test.key, test.tags, v, test.expected)
		}
	}
}

func TestWayZOrder(t *testing.T) {
	zOrder, err := MakeWayZOrder("z_order",
		AvailableColumnTypes["wayzorder"],
		config.Column{
			Name: "zorder",
			Type: "wayzorder",
			Args: map[string]interface{}{
				"default": float64(5),
				"ranks": []interface{}{
					"path",
					"footway",
					"pedestrian",
					"residential",
					"light_rail",
					"primary",
					"tram",
					"rail",
					"trunk",
					"motorway_link",
					"motorway",
				}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	NIL := 999 // marker value
	tests := []struct {
		key      string
		tags     osm.Tags
		expected int
	}{
		{"unknown", nil, 5},
		{"path", nil, 1},
		{"residential", nil, 4},
		{"residential", nil, 4},
		{"motorway", nil, 11},
		{"path", osm.Tags{"bridge": "yes"}, 12},
		{"path", osm.Tags{"layer": "1"}, 12},
		{"path", osm.Tags{"tunnel": "yes"}, -10},
		{"unknown", osm.Tags{"tunnel": "yes"}, -6},
		{"unknown", osm.Tags{"tunnel": "yes", "layer": "1"}, 5},
		{"unknown", osm.Tags{"tunnel": "yes", "layer": "123456789123456789"}, NIL},
	}
	for _, test := range tests {
		elem := &osm.Element{Tags: test.tags}
		match := Match{Value: test.key}

		if test.expected == NIL {
			if v := zOrder("", elem, nil, match); v != nil {
				t.Errorf("%v %v %#v != nil", test.key, test.tags, v)
			}

		} else if v := zOrder("", elem, nil, match); v.(int) != test.expected {
			t.Errorf("%v %v %d != %d", test.key, test.tags, v, test.expected)
		}
	}
}

func TestAreaColumn(t *testing.T) {
	tests := []struct {
		wkt      string
		expected float32
		areaFunc MakeValue
	}{
		{"POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))", 100.0, Area},
		{"POLYGON((-10 0, 10 0, 10 10, -10 10, -10 0))", 200.0, Area},
		{"POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))", 400.0, WebmercArea},
		{"POLYGON((1000000  2000000, 1001000  2000000, 1001000  2001000, 1000000  2001000, 1000000  2000000))", 1000000.0, Area},
		{"POLYGON((1000000  2000000, 1001000  2000000, 1001000  2001000, 1000000  2001000, 1000000  2000000))", 907733.750000, WebmercArea},
		{"POLYGON((1000000  5000000, 1001000  5000000, 1001000  5001000, 1000000  5001000, 1000000  5000000))", 570974.687500, WebmercArea},
		{"POLYGON((1000000 10000000, 1001000 10000000, 1001000 10001000, 1000000 10001000, 1000000 10000000))", 159667.406250, WebmercArea},
		{"POLYGON((1284931 6129149,1284931 6129153,1284931 6129174,1285008 6129171,1285008 6129155,1285008 6129146,1284931 6129149))", 1925.000000, Area},
		{"POLYGON((1284931 6129149,1284931 6129153,1284931 6129174,1285008 6129171,1285008 6129155,1285008 6129146,1284931 6129149))", 857.418396, WebmercArea},
		// 100x100m square between ~20N and ~70N transformed from UTM to Webmerc
		{"POLYGON ((1212900 2099809, 1212900 2099916, 1212794 2099916, 1212794 2099809, 1212900 2099809))", 10196.298828, WebmercArea},
		{"POLYGON ((1227489 3193498, 1227489 3193613, 1227374 3193613, 1227374 3193498, 1227489 3193498))", 10394.006836, WebmercArea},
		{"POLYGON ((1250827 4379962, 1250827 4380090, 1250700 4380090, 1250700 4379962, 1250827 4379962))", 10484.050781, WebmercArea},
		{"POLYGON ((1287373 5712461, 1287373 5712609, 1287226 5712609, 1287226 5712461, 1287373 5712461))", 10659.601562, WebmercArea},
		{"POLYGON ((1346379 7276530, 1346379 7276709, 1346199 7276709, 1346199 7276530, 1346379 7276530))", 10834.080078, WebmercArea},
		{"POLYGON ((1449880 9229305, 1449880 9229543, 1449643 9229543, 1449643 9229305, 1449880 9229305))", 11212.663086, WebmercArea},
		{"POLYGON ((1665035 11920408, 1665035 11920770, 1664673 11920770, 1664673 11920408, 1665035 11920408))", 11903.427734, WebmercArea},
	}
	g := geos.NewGeos()
	for _, test := range tests {
		ggeom := g.FromWkt(test.wkt)
		if ggeom == nil {
			t.Fatalf("unable to create test geometry from %v", test.wkt)
		}
		geometry, err := geom.AsGeomElement(g, ggeom)
		if err != nil {
			t.Fatalf("unable to create test geometry %v: %v", test.wkt, err)
		}
		elem := &osm.Element{}
		match := Match{}

		if v := test.areaFunc("", elem, &geometry, match); v.(float32) != test.expected {
			t.Errorf("%v %f != %f", test.wkt, v, test.expected)
		}
	}
}

func TestMakeSuffixReplace(t *testing.T) {
	column := config.Column{
		Name: "name", Key: "name", Type: "string_suffixreplace",
		Args: map[string]interface{}{"suffixes": map[interface{}]interface{}{"Straße": "Str.", "straße": "str."}}}
	suffixReplace, err := MakeSuffixReplace("name", ColumnType{}, column)

	if err != nil {
		t.Fatal(err)
	}

	if result := suffixReplace("Hauptstraße", nil, nil, Match{}); result != "Hauptstr." {
		t.Fatal(result)
	}
	if result := suffixReplace("", nil, nil, Match{}); result != "" {
		t.Fatal(result)
	}
	if result := suffixReplace("Foostraßeee", nil, nil, Match{}); result != "Foostraßeee" {
		t.Fatal(result)
	}
}

func TestHstoreString(t *testing.T) {
	column := config.Column{
		Name: "tags",
		Type: "hstore_tags",
	}
	hstoreAll, err := MakeHStoreString("tags", ColumnType{}, column)
	if err != nil {
		t.Fatal(err)
	}

	column = config.Column{
		Name: "tags",
		Type: "hstore_tags",
		Args: map[string]interface{}{"include": []interface{}{"key1", "key2"}},
	}
	hstoreInclude, err := MakeHStoreString("tags", ColumnType{}, column)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		column   MakeValue
		tags     osm.Tags
		expected interface{}
	}{
		{hstoreAll, osm.Tags{}, ``},
		{hstoreAll, osm.Tags{"key": "value"}, `"key"=>"value"`},
		{hstoreAll, osm.Tags{`"key"`: `'"value"'`}, `"\"key\""=>"'\"value\"'"`},
		{hstoreAll, osm.Tags{`\`: `\\\\`}, `"\\"=>"\\\\\\\\"`},
		{hstoreAll, osm.Tags{"Ümlåütê=>": ""}, `"Ümlåütê=>"=>""`},
		{hstoreInclude, osm.Tags{"key": "value"}, ``},
		{hstoreInclude, osm.Tags{"key1": "value"}, `"key1"=>"value"`},
		{hstoreInclude, osm.Tags{"key": "value", "key2": "value"}, `"key2"=>"value"`},
	} {
		actual := test.column("", &osm.Element{Tags: test.tags}, nil, Match{})
		if actual.(string) != test.expected {
			t.Errorf("%#v != %#v for %#v", actual, test.expected, test.tags)
		}
	}

	actual := hstoreAll("", &osm.Element{Tags: osm.Tags{"key1": "value", "key2": "value"}}, nil, Match{})
	// check mutliple tags, can be in any order
	if actual.(string) != `"key1"=>"value", "key2"=>"value"` && actual.(string) != `"key2"=>"value", "key1"=>"value"` {
		t.Error("unexpected value", actual)
	}

}

func TestJSONBString(t *testing.T) {
	column := config.Column{
		Name: "tags",
		Type: "jsonb_tags",
	}
	jsonbAll, err := MakeJSONBString("tags", ColumnType{}, column)
	if err != nil {
		t.Fatal(err)
	}

	column = config.Column{
		Name: "tags",
		Type: "jsonb_tags",
		Args: map[string]interface{}{"include": []interface{}{"key1", "key2"}},
	}
	jsonbInclude, err := MakeJSONBString("tags", ColumnType{}, column)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		column   MakeValue
		tags     osm.Tags
		expected interface{}
	}{
		{jsonbAll, osm.Tags{}, `{}`},
		{jsonbAll, osm.Tags{"key": "value"}, `{"key":"value"}`},
		{jsonbAll, osm.Tags{`"key"`: `'"value"'`}, `{"\"key\"":"'\"value\"'"}`},
		{jsonbAll, osm.Tags{`\`: `\\\\`}, `{"\\":"\\\\\\\\"}`},
		{jsonbAll, osm.Tags{"Ümlåütê:": ""}, `{"Ümlåütê:":""}`},
		{jsonbInclude, osm.Tags{"key": "value"}, `{}`},
		{jsonbInclude, osm.Tags{"key1": "value"}, `{"key1":"value"}`},
		{jsonbInclude, osm.Tags{"key": "value", "key2": "value"}, `{"key2":"value"}`},
	} {
		actual := test.column("", &osm.Element{Tags: test.tags}, nil, Match{})
		if actual.(string) != test.expected {
			t.Errorf("%#v != %#v for %#v", actual, test.expected, test.tags)
		}
	}

	actual := jsonbAll("{}", &osm.Element{Tags: osm.Tags{"key1": "value", "key2": "value"}}, nil, Match{})
	// check mutliple tags, can be in any order
	if actual.(string) != `{"key1":"value","key2":"value"}` && actual.(string) != `{"key2":"value","key1":"value"}` {
		t.Error("unexpected value", actual)
	}

}