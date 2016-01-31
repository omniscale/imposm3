package test

/*

Test case:
  create new mapping.FieldType -s based on new pgtypes.

Expected:
  new mappings - on the PostgreSQL table (  mapping: pgtype_test_mapping.json )


Test command with verbose logging:
     godep go test ./test/helper_test.go  ./test/pgtype_test.go -v

*/

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/omniscale/imposm3/config"
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/geom/geos"
	"github.com/omniscale/imposm3/mapping"
)

func init() {

	mapping.RegisterFieldTypes(mapping.FieldType{Name: "example_string_char1", GoType: "char1", Func: getField_test_String1, MakeFunc: nil, MemberFunc: nil, FromMember: false})
	mapping.RegisterFieldTypes(mapping.FieldType{Name: "example_date", GoType: "date", Func: getField_test_date, MakeFunc: nil, MemberFunc: nil, FromMember: false})
	mapping.RegisterFieldTypes(mapping.FieldType{Name: "example_time", GoType: "time", Func: getField_test_time, MakeFunc: nil, MemberFunc: nil, FromMember: false})
	mapping.RegisterFieldTypes(mapping.FieldType{Name: "example_timestamp", GoType: "timestamp", Func: getField_test_timestamp, MakeFunc: nil, MemberFunc: nil, FromMember: false})
	mapping.RegisterFieldTypes(mapping.FieldType{Name: "example_geometry_noindex", GoType: "geometry_noindex", Func: getField_test_geometry_noindex, MakeFunc: nil, MemberFunc: nil, FromMember: false})
	mapping.RegisterFieldTypes(mapping.FieldType{Name: "example_point", GoType: "point", Func: getField_test_point, MakeFunc: nil, MemberFunc: nil, FromMember: false})
	mapping.RegisterFieldTypes(mapping.FieldType{Name: "example_linestring", GoType: "linestring", Func: getField_test_linestring, MakeFunc: nil, MemberFunc: nil, FromMember: false})

}

func getField_test_String1(val string, elem *element.OSMElem, geom *geom.Geometry, match mapping.Match) interface{} {
	return ("O")[:1]
}

func getField_test_date(val string, elem *element.OSMElem, geom *geom.Geometry, match mapping.Match) interface{} {
	return "2016-01-02"
}

func getField_test_time(val string, elem *element.OSMElem, geom *geom.Geometry, match mapping.Match) interface{} {
	return "14:15:16"
}

func getField_test_timestamp(val string, elem *element.OSMElem, geom *geom.Geometry, match mapping.Match) interface{} {
	return "2012-12-22T00:22:11Z"
}

func getField_test_geometry_noindex(val string, elem *element.OSMElem, geom *geom.Geometry, match mapping.Match) interface{} {
	return string(geom.Wkb)
}

func getField_test_point(val string, elem *element.OSMElem, geom *geom.Geometry, match mapping.Match) interface{} {
	g := geos.NewGeos()
	defer g.Finish()
	g.SetHandleSrid(config.BaseOptions.Srid)
	point := g.Clone(g.FromWkt("POINT(44444 55555)"))

	return string(g.AsEwkbHex(point))
}

func getField_test_linestring(val string, elem *element.OSMElem, geom *geom.Geometry, match mapping.Match) interface{} {
	g := geos.NewGeos()
	defer g.Finish()
	g.SetHandleSrid(config.BaseOptions.Srid)
	linestring := g.Clone(g.FromWkt("LINESTRING(33333 55555, 77777 99999)"))

	return string(g.AsEwkbHex(linestring))
}

func (s *importTestSuite) querySqlFilterExists(t *testing.T, table string, sqlfilter string) bool {

	row := s.db.QueryRow(fmt.Sprintf(`SELECT EXISTS(SELECT * FROM %s WHERE %s )`, table, sqlfilter))
	var exists bool
	if err := row.Scan(&exists); err != nil {
		t.Error(err)
		return false
	}
	return exists
}

func TestPgTypes(t *testing.T) {

	ts.dir = "/tmp/imposm3test_pgtype"
	ts.config = importConfig{
		connection:      "postgis://",
		cacheDir:        ts.dir,
		osmFileName:     "build/complete_db.pbf",
		mappingFileName: "pgtype_test_mapping.json",
	}
	ts.g = geos.NewGeos()

	var err error
	ts.db, err = sql.Open("postgres", "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	ts.dropSchemas()

	// =======================================================================
	t.Log("Import - step ")
	if ts.tableExists(t, dbschemaImport, "osm_fieldtype_test") != false {
		t.Fatalf("table osm_fieldtype_test exists in schema %s", dbschemaImport)
	}
	ts.importOsm(t)
	if ts.tableExists(t, dbschemaImport, "osm_fieldtype_test") != true {
		t.Fatalf("table osm_fieldtype_test does not exists in schema %s", dbschemaImport)
	}

	// =======================================================================
	t.Log("Deploy - step ")
	ts.deployOsm(t)
	if ts.tableExists(t, dbschemaImport, "osm_fieldtype_test") != false {
		t.Fatalf("table osm_fieldtype_test exists in schema %s", dbschemaImport)
	}
	if ts.tableExists(t, dbschemaProduction, "osm_fieldtype_test") != true {
		t.Fatalf("table osm_fieldtype_test does not exists in schema %s", dbschemaProduction)
	}

	// =======================================================================
	t.Log("Check - step  ")

	/*
	   select * from geometry_columns where  f_table_catalog='imposm3dev' and f_table_schema='public' and f_table_name='osm_fieldtype_test';
	    f_table_catalog | f_table_schema |    f_table_name    |    f_geometry_column     | coord_dimension | srid |    type
	   -----------------+----------------+--------------------+--------------------------+-----------------+------+------------
	    imposm3dev      | public         | osm_fieldtype_test | geometry                 |               2 | 3857 | GEOMETRY
	    imposm3dev      | public         | osm_fieldtype_test | example_geometry_noindex |               2 | 3857 | GEOMETRY
	    imposm3dev      | public         | osm_fieldtype_test | example_point            |               2 | 3857 | POINT
	    imposm3dev      | public         | osm_fieldtype_test | example_linestring       |               2 | 3857 | LINESTRING
	*/

	t.Log("Check geometry colums ")

	type geomfield struct {
		Name string
		Type string
	}

	// Test   geometry fields from "geometry_columns" table
	geomcolfilter := fmt.Sprintf(` f_table_schema='%s' and f_table_name='osm_fieldtype_test' and srid=3857 `, dbschemaProduction)
	for _, g := range []geomfield{
		{"geometry", "GEOMETRY"},
		{"example_geometry_noindex", "GEOMETRY"},
		{"example_point", "POINT"},
		{"example_linestring", "LINESTRING"},
	} {
		t.Log("check geometry variable: ", g.Name, g.Type)
		if ts.querySqlFilterExists(t, "geometry_columns", geomcolfilter+fmt.Sprintf(` and f_geometry_column ='%s' and  type='%s' `, g.Name, g.Type)) != true {
			t.Fatalf("table osm_fieldtype_test geometry field  does not exists : %s , %s , %s ", dbschemaProduction, g.Name, g.Type)
		}

	}

	// Test fields  based on  "information_schema.columns"  info
	colfilter := fmt.Sprintf(` table_schema='%s' and table_name='osm_fieldtype_test'  `, dbschemaProduction)
	for _, g := range []geomfield{
		{"id", "int4"},
		{"osm_id", "int8"},
		{"admin_level_char1", "char"},
		{"example_date", "date"},
		{"example_time", "time"},
		{"example_timestamp", "timestamp"},
		{"geometry", "geometry"},
		{"example_geometry_noindex", "geometry"},
		{"example_point", "geometry"},
		{"example_linestring", "geometry"},
	} {
		t.Log("check variable: ", g.Name, g.Type)
		if ts.querySqlFilterExists(t, "information_schema.columns ", colfilter+fmt.Sprintf(` and column_name ='%s' and  udt_name='%s' `, g.Name, g.Type)) != true {
			t.Fatalf("table %s.osm_fieldtype_test :: field  does not exists ( %s , %s ) ", dbschemaProduction, g.Name, g.Type)
		}
	}

	t.Log("-- end -- ")
}
