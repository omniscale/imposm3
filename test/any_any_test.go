package test

import (
	"database/sql"
	"io/ioutil"
	"os"

	"testing"

	"github.com/omniscale/imposm3/geom/geos"
)

func TestAnyAny_Prepare(t *testing.T) {
	var err error

	ts.dir, err = ioutil.TempDir("", "imposm3test")
	if err != nil {
		t.Fatal(err)
	}
	ts.config = importConfig{
		connection:      "postgis://",
		cacheDir:        ts.dir,
		osmFileName:     "build/any_any.pbf",
		mappingFileName: "any_any_mapping.json",
	}
	ts.g = geos.NewGeos()

	ts.db, err = sql.Open("postgres", "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	ts.dropSchemas()
}

func TestAnyAny_Import(t *testing.T) {
	if ts.tableExists(t, dbschemaImport, "osm_all") != false {
		t.Fatalf("table osm_all exists in schema %s", dbschemaImport)
	}
	ts.importOsm(t)
	ts.deployOsm(t)
	if ts.tableExists(t, dbschemaProduction, "osm_all") != true {
		t.Fatalf("table osm_all does not exists in schema %s", dbschemaProduction)
	}
}

func TestAnyAny_InsertedNodes(t *testing.T) {
	assertHstore(t, []checkElem{
		{"osm_all", 10000, "", nil}, // nodes without tags are not inserted
		{"osm_all", 10001, "*", map[string]string{"random": "tag"}},
		{"osm_all", 10002, "*", map[string]string{"amenity": "shop"}},
		{"osm_all", 10003, "*", map[string]string{"random": "tag", "but": "mapped", "amenity": "shop"}},
		{"osm_amenities", 10002, "*", map[string]string{"amenity": "shop"}},
		{"osm_amenities", 10003, "*", map[string]string{"random": "tag", "but": "mapped", "amenity": "shop"}},
	})
}

func TestAnyAny_Cleanup(t *testing.T) {
	ts.dropSchemas()
	if err := os.RemoveAll(ts.dir); err != nil {
		t.Error(err)
	}
}
