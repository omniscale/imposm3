package test

import (
	"database/sql"
	"io/ioutil"
	"os"
	"strings"

	"testing"

	"github.com/omniscale/imposm3/geom/geos"
)

const RelOffset = -1e17

func TestSingleTable(t *testing.T) {
	if testing.Short() {
		t.Skip("system test skipped with -test.short")
	}
	t.Parallel()

	ts := importTestSuite{
		name: "single_table",
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
			osmFileName:     "build/single_table.pbf",
			mappingFileName: "single_table_mapping.json",
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
		if ts.tableExists(t, ts.dbschemaImport(), "osm_all") != true {
			t.Fatalf("table osm_all does not exists in schema %s", ts.dbschemaImport())
		}
	})

	t.Run("Deploy", func(t *testing.T) {
		ts.deployOsm(t)
		if ts.tableExists(t, ts.dbschemaImport(), "osm_all") != false {
			t.Fatalf("table osm_all exists in schema %s", ts.dbschemaImport())
		}
		if ts.tableExists(t, ts.dbschemaProduction(), "osm_all") != true {
			t.Fatalf("table osm_all does not exists in schema %s", ts.dbschemaProduction())
		}
	})

	t.Run("NonMappedNodeIsMissing", func(t *testing.T) {
		// Node without mapped tags is missing.
		cache := ts.cache(t)
		defer cache.Close()
		ts.assertCachedNode(t, cache, 10001)

		ts.assertHstore(t, []checkElem{
			{"osm_all", 10001, Missing, nil},
		})
	})

	t.Run("MappedNode", func(t *testing.T) {
		// Node is stored with all tags.
		cache := ts.cache(t)
		defer cache.Close()
		ts.assertCachedNode(t, cache, 10002)

		ts.assertHstore(t, []checkElem{
			{"osm_all", 10002, "*", map[string]string{"random": "tag", "but": "mapped", "poi": "unicorn"}},
		})
	})

	t.Run("NonMappedWayIsMissing", func(t *testing.T) {
		// Way without mapped tags is missing.
		cache := ts.cache(t)
		defer cache.Close()
		ts.assertCachedWay(t, cache, 20101)
		ts.assertCachedWay(t, cache, 20102)
		ts.assertCachedWay(t, cache, 20103)

		ts.assertHstore(t, []checkElem{
			{"osm_all", 20101, Missing, nil},
			{"osm_all", 20102, Missing, nil},
			{"osm_all", 20103, Missing, nil},
		})
	})

	t.Run("MappedWay", func(t *testing.T) {
		// Way is stored with all tags.
		cache := ts.cache(t)
		defer cache.Close()
		ts.assertCachedWay(t, cache, 20201)

		ts.assertHstore(t, []checkElem{
			{"osm_all", -20201, "*", map[string]string{"random": "tag", "highway": "yes"}},
		})
		ts.assertGeomLength(t, checkElem{"osm_all", -20201, "*", nil}, 111319.5)
	})

	t.Run("NonMappedClosedWayIsMissing", func(t *testing.T) {
		// Closed way without mapped tags is missing.
		cache := ts.cache(t)
		defer cache.Close()
		ts.assertCachedWay(t, cache, 20301)
		ts.assertHstore(t, []checkElem{
			{"osm_all", 20301, Missing, nil},
			{"osm_all", -20301, Missing, nil},
		})
	})

	t.Run("MappedClosedWay", func(t *testing.T) {
		// Closed way is stored with all tags.
		ts.assertHstore(t, []checkElem{
			{"osm_all", -20401, "*", map[string]string{"random": "tag", "building": "yes"}},
		})
	})

	t.Run("MappedClosedWayAreaYes", func(t *testing.T) {
		// Closed way with area=yes is not stored as linestring.
		ts.assertHstore(t, []checkElem{
			{"osm_all", -20501, "*", map[string]string{"random": "tag", "landuse": "grass", "highway": "pedestrian", "area": "yes"}},
		})
		ts.assertGeomType(t, checkElem{"osm_all", -20501, "*", nil}, "Polygon")
	})

	t.Run("MappedClosedWayAreaNo", func(t *testing.T) {
		// Closed way with area=no is not stored as polygon.
		ts.assertHstore(t, []checkElem{
			{"osm_all", -20502, "*", map[string]string{"random": "tag", "landuse": "grass", "highway": "pedestrian", "area": "no"}},
		})
		ts.assertGeomType(t, checkElem{"osm_all", -20502, "*", nil}, "LineString")
	})

	t.Run("MappedClosedWayWithoutArea", func(t *testing.T) {
		// Closed way without area is stored as mapped (linestring and polygon).

		rows := ts.queryRowsTags(t, "osm_all", -20601)
		if len(rows) != 2 || strings.HasPrefix(rows[0].wkt, "LineString") || strings.HasPrefix(rows[1].wkt, "Polygon") {
			t.Errorf("unexpected geometries: %v", rows)
		}
	})

	t.Run("DuplicateIds1", func(t *testing.T) {
		// Points/lines/polygons with same ID are inserted.

		ts.assertHstore(t, []checkElem{
			{"osm_all", 31101, "*", map[string]string{"amenity": "cafe"}},
		})

		rows := ts.queryRowsTags(t, "osm_all", -31101)
		if len(rows) != 2 || strings.HasPrefix(rows[0].wkt, "LineString") || strings.HasPrefix(rows[1].wkt, "Polygon") {
			t.Errorf("unexpected geometries: %v", rows)
		}

		ts.assertHstore(t, []checkElem{
			{"osm_all", RelOffset - 31101, "*", map[string]string{"building": "yes", "type": "multipolygon"}},
		})
		ts.assertGeomType(t, checkElem{"osm_all", RelOffset - 31101, "*", nil}, "Polygon")
	})

	// #######################################################################

	t.Run("Update", func(t *testing.T) {
		ts.updateOsm(t, "build/single_table.osc.gz")
	})

	// #######################################################################

	t.Run("DuplicateIds2", func(t *testing.T) {
		// Node moved and ways/rels with same ID are still present.

		ts.assertHstore(t, []checkElem{
			{"osm_all", 31101, "*", map[string]string{"amenity": "cafe"}},
		})

		rows := ts.queryRowsTags(t, "osm_all", -31101)
		if len(rows) != 2 || strings.HasPrefix(rows[0].wkt, "LineString") || strings.HasPrefix(rows[1].wkt, "Polygon") {
			t.Errorf("unexpected geometries: %v", rows)
		}

		ts.assertHstore(t, []checkElem{
			{"osm_all", RelOffset - 31101, "*", map[string]string{"building": "yes", "type": "multipolygon"}},
		})
		ts.assertGeomType(t, checkElem{"osm_all", RelOffset - 31101, "*", nil}, "Polygon")
	})

	t.Run("ModifiedRelation2", func(t *testing.T) {
		// Modified relation is not inserted twice. Check for #88

		rows := ts.queryRowsTags(t, "osm_all", RelOffset-32901)
		if len(rows) != 1 {
			t.Errorf("found duplicate row: %v", rows)
		}
	})

	t.Run("ModifiedWayGeometryAfterNodeMoved", func(t *testing.T) {
		ts.assertGeomLength(t, checkElem{"osm_all", -20201, "*", nil}, 222639)
	})

	t.Run("Cleanup", func(t *testing.T) {
		ts.dropSchemas()
		if err := os.RemoveAll(ts.dir); err != nil {
			t.Error(err)
		}
	})
}
