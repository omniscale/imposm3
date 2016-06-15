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

func TestSingleTable_Prepare(t *testing.T) {
	var err error

	ts.dir, err = ioutil.TempDir("", "imposm3test")
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
}

func TestSingleTable_Import(t *testing.T) {
	if ts.tableExists(t, dbschemaImport, "osm_all") != false {
		t.Fatalf("table osm_all exists in schema %s", dbschemaImport)
	}
	ts.importOsm(t)
	if ts.tableExists(t, dbschemaImport, "osm_all") != true {
		t.Fatalf("table osm_all does not exists in schema %s", dbschemaImport)
	}
}

func TestSingleTable_Deploy(t *testing.T) {
	ts.deployOsm(t)
	if ts.tableExists(t, dbschemaImport, "osm_all") != false {
		t.Fatalf("table osm_all exists in schema %s", dbschemaImport)
	}
	if ts.tableExists(t, dbschemaProduction, "osm_all") != true {
		t.Fatalf("table osm_all does not exists in schema %s", dbschemaProduction)
	}
}

func TestSingleTable_NonMappedNodeIsMissing(t *testing.T) {
	// Node without mapped tags is missing.
	cache := ts.cache(t)
	defer cache.Close()
	assertCachedNode(t, cache, 10001)

	assertHstore(t, []checkElem{
		{"osm_all", 10001, Missing, nil},
	})
}

func TestSingleTable_MappedNode(t *testing.T) {
	// Node is stored with all tags.
	cache := ts.cache(t)
	defer cache.Close()
	assertCachedNode(t, cache, 10002)

	assertHstore(t, []checkElem{
		{"osm_all", 10002, "*", map[string]string{"random": "tag", "but": "mapped", "poi": "unicorn"}},
	})
}

func TestSingleTable_NonMappedWayIsMissing(t *testing.T) {
	// Way without mapped tags is missing.
	cache := ts.cache(t)
	defer cache.Close()
	assertCachedWay(t, cache, 20101)
	assertCachedWay(t, cache, 20102)
	assertCachedWay(t, cache, 20103)

	assertHstore(t, []checkElem{
		{"osm_all", 20101, Missing, nil},
		{"osm_all", 20102, Missing, nil},
		{"osm_all", 20103, Missing, nil},
	})
}

func TestSingleTable_MappedWay(t *testing.T) {
	// Way is stored with all tags.
	cache := ts.cache(t)
	defer cache.Close()
	assertCachedWay(t, cache, 20201)

	assertHstore(t, []checkElem{
		{"osm_all", -20201, "*", map[string]string{"random": "tag", "highway": "yes"}},
	})
}

func TestSingleTable_NonMappedClosedWayIsMissing(t *testing.T) {
	// Closed way without mapped tags is missing.
	cache := ts.cache(t)
	defer cache.Close()
	assertCachedWay(t, cache, 20301)
	assertHstore(t, []checkElem{
		{"osm_all", 20301, Missing, nil},
		{"osm_all", -20301, Missing, nil},
	})
}

func TestSingleTable_MappedClosedWay(t *testing.T) {
	// Closed way is stored with all tags.
	assertHstore(t, []checkElem{
		{"osm_all", -20401, "*", map[string]string{"random": "tag", "building": "yes"}},
	})
}

func TestSingleTable_MappedClosedWayAreaYes(t *testing.T) {
	// Closed way with area=yes is not stored as linestring.
	assertHstore(t, []checkElem{
		{"osm_all", -20501, "*", map[string]string{"random": "tag", "landuse": "grass", "highway": "pedestrian", "area": "yes"}},
	})
	assertGeomType(t, checkElem{"osm_all", -20501, "*", nil}, "Polygon")
}

func TestSingleTable_MappedClosedWayAreaNo(t *testing.T) {
	// Closed way with area=no is not stored as polygon.
	assertHstore(t, []checkElem{
		{"osm_all", -20502, "*", map[string]string{"random": "tag", "landuse": "grass", "highway": "pedestrian", "area": "no"}},
	})
	assertGeomType(t, checkElem{"osm_all", -20502, "*", nil}, "LineString")
}

func TestSingleTable_MappedClosedWayWithoutArea(t *testing.T) {
	// Closed way without area is stored as mapped (linestring and polygon).

	rows := ts.queryRowsTags(t, "osm_all", -20601)
	if len(rows) != 2 || strings.HasPrefix(rows[0].wkt, "LineString") || strings.HasPrefix(rows[1].wkt, "Polygon") {
		t.Errorf("unexpected geometries: %v", rows)
	}
}

func TestSingleTable_DuplicateIds1(t *testing.T) {
	// Points/lines/polygons with same ID are inserted.

	assertHstore(t, []checkElem{
		{"osm_all", 31101, "*", map[string]string{"amenity": "cafe"}},
	})

	rows := ts.queryRowsTags(t, "osm_all", -31101)
	if len(rows) != 2 || strings.HasPrefix(rows[0].wkt, "LineString") || strings.HasPrefix(rows[1].wkt, "Polygon") {
		t.Errorf("unexpected geometries: %v", rows)
	}

	assertHstore(t, []checkElem{
		{"osm_all", RelOffset - 31101, "*", map[string]string{"building": "yes"}},
	})
	assertGeomType(t, checkElem{"osm_all", RelOffset - 31101, "*", nil}, "Polygon")
}

// #######################################################################

func TestSingleTable_Update(t *testing.T) {
	ts.updateOsm(t, "./build/single_table.osc.gz")
}

// #######################################################################

func TestSingleTable_DuplicateIds2(t *testing.T) {
	// Node moved and ways/rels with same ID are still present.

	assertHstore(t, []checkElem{
		{"osm_all", 31101, "*", map[string]string{"amenity": "cafe"}},
	})

	rows := ts.queryRowsTags(t, "osm_all", -31101)
	if len(rows) != 2 || strings.HasPrefix(rows[0].wkt, "LineString") || strings.HasPrefix(rows[1].wkt, "Polygon") {
		t.Errorf("unexpected geometries: %v", rows)
	}

	assertHstore(t, []checkElem{
		{"osm_all", RelOffset - 31101, "*", map[string]string{"building": "yes"}},
	})
	assertGeomType(t, checkElem{"osm_all", RelOffset - 31101, "*", nil}, "Polygon")
}

func TestSingleTable_ModifiedRelation2(t *testing.T) {
	// Modified relation is not inserted twice. Check for #88

	rows := ts.queryRowsTags(t, "osm_all", RelOffset-32901)
	if len(rows) != 1 {
		t.Errorf("found duplicate row: %v", rows)
	}
}

func TestSingleTable_Cleanup(t *testing.T) {
	ts.dropSchemas()
	if err := os.RemoveAll(ts.dir); err != nil {
		t.Error(err)
	}
}
