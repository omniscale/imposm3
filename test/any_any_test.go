package test

import (
	"database/sql"
	"io/ioutil"
	"os"

	"testing"

	"github.com/omniscale/imposm3/geom/geos"
)

func TestAnyAny(t *testing.T) {
	if testing.Short() {
		t.Skip("system test skipped with -test.short")
	}

	ts := importTestSuite{
		name: "any_any",
	}

	t.Run("Prepare", func(t *testing.T) {
		var err error

		ts.dir, err = ioutil.TempDir("", "imposm_test")
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
	})

	t.Run("Import", func(t *testing.T) {
		if ts.tableExists(t, ts.dbschemaImport(), "osm_all") != false {
			t.Fatalf("table osm_all exists in schema %s", ts.dbschemaImport())
		}
		ts.importOsm(t)
		ts.deployOsm(t)
		if ts.tableExists(t, ts.dbschemaProduction(), "osm_all") != true {
			t.Fatalf("table osm_all does not exists in schema %s", ts.dbschemaProduction())
		}
	})

	t.Run("InsertedNodes", func(t *testing.T) {
		ts.assertHstore(t, []checkElem{
			{"osm_all", 10000, "", nil}, // nodes without tags are not inserted
			{"osm_all", 10001, "*", map[string]string{"random": "tag"}},
			{"osm_all", 10002, "*", map[string]string{"amenity": "shop"}},
			{"osm_all", 10003, "*", map[string]string{"random": "tag", "but": "mapped", "amenity": "shop"}},
			{"osm_amenities", 10002, "*", map[string]string{"amenity": "shop"}},
			{"osm_amenities", 10003, "*", map[string]string{"random": "tag", "but": "mapped", "amenity": "shop"}},
		})
	})

	t.Run("Cleanup", func(t *testing.T) {
		ts.dropSchemas()
		if err := os.RemoveAll(ts.dir); err != nil {
			t.Error(err)
		}
	})
}
