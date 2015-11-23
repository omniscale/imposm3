package test

import (
	"database/sql"

	"testing"

	"github.com/omniscale/imposm3/geom/geos"
)

const RelOffset = -1e17

func TestSingleTable_Prepare(t *testing.T) {
	ts.dir = "/tmp/imposm3test"
	ts.config = importConfig{
		connection:      "postgis://",
		cacheDir:        ts.dir,
		osmFileName:     "build/single_table.pbf",
		mappingFileName: "single_table_mapping.json",
	}
	ts.g = geos.NewGeos()

	var err error
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
	// t.assert_cached_node(10001, (10, 42))

	assertHstore(t, []checkElem{
		{"osm_all", 10001, "", nil},
	})
}

func TestSingleTable_MappedNode(t *testing.T) {
	// Node is stored with all tags.
	// t.assert_cached_node(10002, (11, 42))

	assertHstore(t, []checkElem{
		{"osm_all", 10002, "*", map[string]string{"random": "tag", "but": "mapped", "poi": "unicorn"}},
	})
}

func TestSingleTable_NonMappedWayIsMissing(t *testing.T) {
	// Way without mapped tags is missing.
	// t.assert_cached_way(20101)
	// t.assert_cached_way(20102)
	// t.assert_cached_way(20103)
	assertHstore(t, []checkElem{
		{"osm_all", 20101, "", nil},
		{"osm_all", 20102, "", nil},
		{"osm_all", 20103, "", nil},
	})
}

func TestSingleTable_MappedWay(t *testing.T) {
	// Way is stored with all tags.
	// t.assert_cached_way(20201)
	assertHstore(t, []checkElem{
		{"osm_all", -20201, "*", map[string]string{"random": "tag", "highway": "yes"}},
	})
}

func TestSingleTable_NonMappedClosedWayIsMissing(t *testing.T) {
	// Closed way without mapped tags is missing.
	// t.assert_cached_way(20301)
	assertHstore(t, []checkElem{
		{"osm_all", -20301, "", nil},
	})
}

func TestSingleTable_MappedClosedWay(t *testing.T) {
	// Closed way is stored with all tags.
	// t.assert_cached_way(20401)
	assertHstore(t, []checkElem{
		{"osm_all", -20401, "*", map[string]string{"random": "tag", "building": "yes"}},
	})
}

func TestSingleTable_MappedClosedWayAreaYes(t *testing.T) {
	// Closed way with area=yes is not stored as linestring.
	// t.assert_cached_way(20501)
	assertHstore(t, []checkElem{
		{"osm_all", -20501, "*", map[string]string{"random": "tag", "landuse": "grass", "highway": "pedestrian", "area": "yes"}},
	})
	assertGeomType(t, checkElem{"osm_all", -20501, "*", nil}, "Polygon")
}

func TestSingleTable_MappedClosedWayAreaNo(t *testing.T) {
	// Closed way with area=no is not stored as polygon.
	// t.assert_cached_way(20502)
	assertHstore(t, []checkElem{
		{"osm_all", -20502, "*", map[string]string{"random": "tag", "landuse": "grass", "highway": "pedestrian", "area": "no"}},
	})
	assertGeomType(t, checkElem{"osm_all", -20502, "*", nil}, "LineString")
}

func TestSingleTable_MappedClosedWayWithoutArea(t *testing.T) {
	// Closed way without area is stored as mapped (linestring and polygon).
	// t.assert_cached_way(20601)
	// elems = t.query_row(t.db_conf, 'osm_all', -20601)
	// assert len(elems) == 2
	// elems.sort(key=lambda x: x['geometry'].type)

	// assert elems[0]['geometry'].type == 'LineString', elems[0]['geometry'].type
	// assert elems[0]['tags'] == {'random': 'tag', 'landuse': 'grass', 'highway': 'pedestrian'}
	// assert elems[1]['geometry'].type == 'Polygon', elems[1]['geometry'].type
	// assert elems[1]['tags'] == {'random': 'tag', 'landuse': 'grass', 'highway': 'pedestrian'}
}

func TestSingleTable_DuplicateIds1(t *testing.T) {
	// Points/lines/polygons with same ID are inserted.
	// node = t.query_row(t.db_conf, 'osm_all', 31101)
	// assert node['geometry'].type == 'Point', node['geometry'].type
	// assert node['tags'] == {'amenity': 'cafe'}
	// assert node['geometry'].distance(t.merc_point(80, 47)) < 1

	// ways = t.query_row(t.db_conf, 'osm_all', -31101)
	// ways.sort(key=lambda x: x['geometry'].type)
	// assert ways[0]['geometry'].type == 'LineString', ways[0]['geometry'].type
	// assert ways[0]['tags'] == {'landuse': 'park', 'highway': 'secondary'}
	// assert ways[1]['geometry'].type == 'Polygon', ways[1]['geometry'].type
	// assert ways[1]['tags'] == {'landuse': 'park', 'highway': 'secondary'}

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

	// node = t.query_row(t.db_conf, 'osm_all', 31101)
	// assert node['geometry'].type == 'Point', node['geometry'].type
	// assert node['tags'] == {'amenity': 'cafe'}
	// assert node['geometry'].distance(t.merc_point(81, 47)) < 1

	// ways = t.query_row(t.db_conf, 'osm_all', -31101)
	// ways.sort(key=lambda x: x['geometry'].type)

	// assert ways[0]['geometry'].type == 'LineString', ways[0]['geometry'].type
	// assert ways[0]['tags'] == {'landuse': 'park', 'highway': 'secondary'}
	// assert ways[1]['geometry'].type == 'Polygon', ways[1]['geometry'].type
	// assert ways[1]['tags'] == {'landuse': 'park', 'highway': 'secondary'}

	assertHstore(t, []checkElem{
		{"osm_all", RelOffset - 31101, "*", map[string]string{"building": "yes"}},
	})
	assertGeomType(t, checkElem{"osm_all", RelOffset - 31101, "*", nil}, "Polygon")
}
