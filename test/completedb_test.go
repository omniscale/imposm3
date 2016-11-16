package test

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/omniscale/imposm3/cache"

	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/proj"

	"testing"

	"github.com/omniscale/imposm3/geom/geos"
)

var ts importTestSuite

func TestComplete_Prepare(t *testing.T) {
	var err error

	ts.dir, err = ioutil.TempDir("", "imposm3test")
	if err != nil {
		t.Fatal(err)
	}
	ts.config = importConfig{
		connection:      "postgis://",
		cacheDir:        ts.dir,
		osmFileName:     "build/complete_db.pbf",
		mappingFileName: "complete_db_mapping.json",
	}
	ts.g = geos.NewGeos()

	ts.db, err = sql.Open("postgres", "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	ts.dropSchemas()
}

func TestComplete_Import(t *testing.T) {
	if ts.tableExists(t, dbschemaImport, "osm_roads") != false {
		t.Fatalf("table osm_roads exists in schema %s", dbschemaImport)
	}
	ts.importOsm(t)
	if ts.tableExists(t, dbschemaImport, "osm_roads") != true {
		t.Fatalf("table osm_roads does not exists in schema %s", dbschemaImport)
	}
}

func TestComplete_Deploy(t *testing.T) {
	ts.deployOsm(t)
	if ts.tableExists(t, dbschemaImport, "osm_roads") != false {
		t.Fatalf("table osm_roads exists in schema %s", dbschemaImport)
	}
	if ts.tableExists(t, dbschemaProduction, "osm_roads") != true {
		t.Fatalf("table osm_roads does not exists in schema %s", dbschemaProduction)
	}
}

func TestComplete_LandusageToWaterarea1(t *testing.T) {
	// Parks inserted into landusages
	cache := ts.cache(t)
	defer cache.Close()
	assertCachedWay(t, cache, 11001)
	assertCachedWay(t, cache, 12001)
	assertCachedWay(t, cache, 13001)

	assertRecords(t, []checkElem{
		{"osm_waterareas", 11001, Missing, nil},
		{"osm_waterareas", -12001, Missing, nil},
		{"osm_waterareas", -13001, Missing, nil},

		{"osm_waterareas_gen0", 11001, Missing, nil},
		{"osm_waterareas_gen0", -12001, Missing, nil},
		{"osm_waterareas_gen0", -13001, Missing, nil},

		{"osm_waterareas_gen1", 11001, Missing, nil},
		{"osm_waterareas_gen1", -12001, Missing, nil},
		{"osm_waterareas_gen1", -13001, Missing, nil},

		{"osm_landusages", 11001, "park", nil},
		{"osm_landusages", -12001, "park", nil},
		{"osm_landusages", -13001, "park", nil},

		{"osm_landusages_gen0", 11001, "park", nil},
		{"osm_landusages_gen0", -12001, "park", nil},
		{"osm_landusages_gen0", -13001, "park", nil},

		{"osm_landusages_gen1", 11001, "park", nil},
		{"osm_landusages_gen1", -12001, "park", nil},
		{"osm_landusages_gen1", -13001, "park", nil},
	})
}

func TestComplete_ChangedHoleTags1(t *testing.T) {
	// Multipolygon relation with untagged hole
	cache := ts.cache(t)
	defer cache.Close()
	assertCachedWay(t, cache, 14001)
	assertCachedWay(t, cache, 14011)

	assertRecords(t, []checkElem{
		{"osm_waterareas", 14011, Missing, nil},
		{"osm_waterareas", -14011, Missing, nil},
		{"osm_landusages", -14001, "park", nil},
	})
}

func TestComplete_SplitOuterMultipolygonWay1(t *testing.T) {
	// Single outer way of multipolygon was inserted.
	assertRecords(t, []checkElem{
		{"osm_roads", 15002, Missing, nil},
		{"osm_landusages", -15001, "park", nil},
	})
	assertGeomArea(t, checkElem{"osm_landusages", -15001, "park", nil}, 9816216452)
}

func TestComplete_MergeOuterMultipolygonWay1(t *testing.T) {
	// Splitted outer way of multipolygon was inserted.
	assertRecords(t, []checkElem{
		{"osm_landusages", -16001, "park", nil},
		{"osm_roads", 16002, "residential", nil},
	})
	assertGeomArea(t, checkElem{"osm_landusages", -16001, "park", nil}, 12779350582)
}

func TestComplete_BrokenMultipolygonWays(t *testing.T) {
	// MultiPolygons with broken outer ways are handled.
	// outer way does not merge (17002 has one node)
	assertRecords(t, []checkElem{
		{"osm_landusages", -17001, Missing, nil},
		{"osm_roads", 17001, "residential", nil},
		{"osm_roads", 17002, Missing, nil},
	})

	// outer way does not merge (17102 has no nodes)
	assertRecords(t, []checkElem{
		{"osm_landusages", -17101, Missing, nil},
		{"osm_roads", 17101, "residential", nil},
		{"osm_roads", 17102, Missing, nil},
	})
}

func TestComplete_NodeWayInsertedTwice(t *testing.T) {
	// Way with multiple mappings is inserted twice in same table
	rows := ts.queryRows(t, "osm_roads", 18001)
	if len(rows) != 2 || rows[0].osmType != "residential" || rows[1].osmType != "tram" {
		t.Errorf("unexpected roads: %v", rows)
	}
}

func TestComplete_OuterWayNotInserted(t *testing.T) {
	// Outer way with different tag is not inserted twice into same table
	assertRecords(t, []checkElem{
		{"osm_landusages", -19001, "farmland", nil},
		{"osm_landusages", 19002, "farmyard", nil},
		{"osm_landusages", 19001, Missing, nil},
	})
}

func TestComplete_OuterWayInserted(t *testing.T) {
	// Outer way with different tag is inserted twice into different table
	assertRecords(t, []checkElem{
		{"osm_landusages", 19101, "farm", nil},
		{"osm_landusages", 19102, "farmyard", nil},
		{"osm_admin", -19101, "administrative", nil},
	})
}

func TestComplete_NodeWayRefAfterDelete1(t *testing.T) {
	// Nodes references way

	cache := ts.diffCache(t)
	defer cache.Close()
	if ids := cache.Coords.Get(20001); len(ids) != 1 || ids[0] != 20001 {
		t.Error("node does not references way")
	}
	if ids := cache.Coords.Get(20002); len(ids) != 1 || ids[0] != 20001 {
		t.Error("node does not references way")
	}

	assertRecords(t, []checkElem{
		{"osm_roads", 20001, "residential", nil},
		{"osm_barrierpoints", 20001, "block", nil},
	})
}

func TestComplete_WayRelRefAfterDelete1(t *testing.T) {
	// Ways references relation

	cache := ts.diffCache(t)
	defer cache.Close()
	if ids := cache.Ways.Get(21001); len(ids) != 1 || ids[0] != 21001 {
		t.Error("way does not references relation")
	}

	assertRecords(t, []checkElem{
		{"osm_roads", 21001, "residential", nil},
		{"osm_landusages", -21001, "park", nil},
	})
}

func TestComplete_RelationWayNotInserted(t *testing.T) {
	// Part of relation was inserted only once.
	assertRecords(t, []checkElem{
		{"osm_landusages", -9001, "park", map[string]string{"name": "rel 9001"}},
		{"osm_landusages", 9009, Missing, nil},
		{"osm_landusages", -9101, "park", map[string]string{"name": "rel 9101"}},
		{"osm_landusages", 9109, Missing, nil},
		{"osm_landusages", 9110, "scrub", nil},
	})
}

func TestComplete_RelationWaysInserted(t *testing.T) {
	// Outer ways of multipolygon are inserted.
	assertRecords(t, []checkElem{
		{"osm_landusages", -9201, "park", map[string]string{"name": "9209"}},
		{"osm_landusages", 9201, Missing, nil},
		// outer ways of multipolygon stand for their own
		{"osm_roads", 9209, "secondary", map[string]string{"name": "9209"}},
		{"osm_roads", 9210, "residential", map[string]string{"name": "9210"}},

		// no name on relation
		{"osm_landusages", -9301, "park", map[string]string{"name": ""}},
		// outer ways of multipolygon stand for their own
		{"osm_roads", 9309, "secondary", map[string]string{"name": "9309"}},
		{"osm_roads", 9310, "residential", map[string]string{"name": "9310"}},
	})

}

func TestComplete_RelationWayInserted(t *testing.T) {
	// Part of relation was inserted twice.
	assertRecords(t, []checkElem{
		{"osm_landusages", -8001, "park", map[string]string{"name": "rel 8001"}},
		{"osm_roads", 8009, "residential", nil},
	})
}

func TestComplete_SingleNodeWaysNotInserted(t *testing.T) {
	// Ways with single/duplicate nodes are not inserted.
	assertRecords(t, []checkElem{
		{"osm_landusages", 30001, Missing, nil},
		{"osm_landusages", 30002, Missing, nil},
		{"osm_landusages", 30003, Missing, nil},
	})
}

func TestComplete_PolygonWithDuplicateNodesIsValid(t *testing.T) {
	// Polygon with duplicate nodes is valid.
	assertGeomValid(t, checkElem{"osm_landusages", 30005, "park", nil})
}

func TestComplete_IncompletePolygons(t *testing.T) {
	// Non-closed/incomplete polygons are not inserted.

	assertRecords(t, []checkElem{
		{"osm_landusages", 30004, Missing, nil},
		{"osm_landusages", 30006, Missing, nil},
	})
}

func TestComplete_ResidentialToSecondary(t *testing.T) {
	// Residential road is not in roads_gen0/1.

	assertRecords(t, []checkElem{
		{"osm_roads", 40001, "residential", nil},
		{"osm_roads_gen0", 40001, Missing, nil},
		{"osm_roads_gen1", 40002, Missing, nil},
	})
}

func TestComplete_RelationBeforeRemove(t *testing.T) {
	// Relation and way is inserted.

	assertRecords(t, []checkElem{
		{"osm_buildings", 50011, "yes", nil},
		{"osm_landusages", -50021, "park", nil},
	})
}

func TestComplete_RelationWithoutTags(t *testing.T) {
	// Relation without tags is inserted.

	assertRecords(t, []checkElem{
		{"osm_buildings", 50111, Missing, nil},
		{"osm_buildings", -50121, "yes", nil},
	})
}

func TestComplete_DuplicateIds(t *testing.T) {
	// Relation/way with same ID is inserted.

	assertRecords(t, []checkElem{
		{"osm_buildings", 51001, "way", nil},
		{"osm_buildings", -51001, "mp", nil},
		{"osm_buildings", 51011, "way", nil},
		{"osm_buildings", -51011, "mp", nil},
	})
}

func TestComplete_RelationUpdatedByNode(t *testing.T) {
	// Relations was updated after modified node.

	assertGeomArea(t, checkElem{"osm_buildings", -52121, "yes", nil}, 13653930440.868315)
}

func TestComplete_GeneralizedBananaPolygonIsValid(t *testing.T) {
	// Generalized polygons are valid.

	assertGeomValid(t, checkElem{"osm_landusages", 7101, Missing, nil})
	// simplified geometies are valid too
	assertGeomValid(t, checkElem{"osm_landusages_gen0", 7101, Missing, nil})
	assertGeomValid(t, checkElem{"osm_landusages_gen1", 7101, Missing, nil})
}

func TestComplete_GeneralizedLinestringIsValid(t *testing.T) {
	// Generalized linestring is valid.

	// geometry is not simple, but valid
	assertGeomLength(t, checkElem{"osm_roads", 7201, "primary", nil}, 1243660.044819)
	if ts.g.IsSimple(ts.queryGeom(t, "osm_roads", 7201)) {
		t.Errorf("expected non-simple geometry for 7201")
	}
	// check that geometry 'survives' simplification
	assertGeomLength(t, checkElem{"osm_roads_gen0", 7201, "primary", nil}, 1243660.044819)
	assertGeomLength(t, checkElem{"osm_roads_gen1", 7201, "primary", nil}, 1243660.044819)
}

func TestComplete_RingWithGap(t *testing.T) {
	// Multipolygon and way with gap (overlapping but different endpoints) gets closed
	assertGeomValid(t, checkElem{"osm_landusages", -7301, Missing, nil})
	assertGeomValid(t, checkElem{"osm_landusages", 7311, Missing, nil})
}

func TestComplete_MultipolygonWithOpenRing(t *testing.T) {
	// Multipolygon is inserted even if there is an open ring/member
	assertGeomValid(t, checkElem{"osm_landusages", -7401, Missing, nil})
}

func TestComplete_UpdatedNodes1(t *testing.T) {
	// Zig-Zag line is inserted.
	assertGeomLength(t, checkElem{"osm_roads", 60000, Missing, nil}, 14035.61150207768)
}

func TestComplete_UpdateNodeToCoord1(t *testing.T) {
	// Node is inserted with tag.
	assertRecords(t, []checkElem{
		{"osm_amenities", 70001, "police", nil},
		{"osm_amenities", 70002, Missing, nil},
	})
}

func TestComplete_EnumerateKey(t *testing.T) {
	// Enumerate from key.
	assertRecords(t, []checkElem{
		{"osm_landusages", 100001, "park", map[string]string{"enum": "1"}},
		{"osm_landusages", 100002, "park", map[string]string{"enum": "0"}},
		{"osm_landusages", 100003, "wood", map[string]string{"enum": "15"}},
	})
}

func TestComplete_AreaMapping(t *testing.T) {
	// Mapping type dependent area-defaults.

	assertRecords(t, []checkElem{
		// highway=pedestrian
		{"osm_roads", 301151, "pedestrian", nil},
		{"osm_landusages", 301151, Missing, nil},

		// // highway=pedestrian, area=yes
		{"osm_roads", 301152, Missing, nil},
		{"osm_landusages", 301152, "pedestrian", nil},

		// // leisure=track
		{"osm_roads", 301153, Missing, nil},
		{"osm_landusages", 301153, "track", nil},

		// // leisure=track, area=no
		{"osm_roads", 301154, "track", nil},
		{"osm_landusages", 301154, Missing, nil},
	})
}

func TestComplete_HstoreTags(t *testing.T) {
	// Mapping type dependent area-defaults.

	assertHstore(t, []checkElem{
		{"osm_buildings", 401151, "*", map[string]string{"amenity": "fuel", "opening_hours": "24/7"}},
	})
}

// #######################################################################

func TestComplete_Update(t *testing.T) {
	ts.updateOsm(t, "./build/complete_db.osc.gz")
}

// #######################################################################

func TestComplete_NoDuplicates(t *testing.T) {
	// Relations/ways are only inserted once Checks #66

	for _, table := range []string{"osm_roads", "osm_landusages"} {
		rows, err := ts.db.Query(
			fmt.Sprintf(`SELECT osm_id, count(osm_id) FROM "%s"."%s" GROUP BY osm_id HAVING count(osm_id) > 1`,
				dbschemaProduction, table))
		if err != nil {
			t.Fatal(err)
		}
		var osmId, count int64
		for rows.Next() {
			if err := rows.Scan(&osmId, &count); err != nil {
				t.Fatal(err)
			}
			if table == "osm_roads" && osmId == 18001 {
				// # duplicate for TestNodeWayInsertedTwice is expected
				if count != 2 {
					t.Error("highway not inserted twice", osmId, count)
				}
			} else {
				t.Error("found duplicate way in osm_roads", osmId, count)
			}
		}
	}
}

func TestComplete_UpdatedLandusage(t *testing.T) {
	// Multipolygon relation was modified

	nd := element.Node{Long: 13.4, Lat: 47.5}
	proj.NodeToMerc(&nd)
	point, err := geom.Point(ts.g, nd)
	if err != nil {
		t.Fatal(err)
	}
	poly := ts.queryGeom(t, "osm_landusages", -1001)
	// point not in polygon after update
	if ts.g.Intersects(point, poly) {
		t.Error("point intersects polygon")
	}
}

func TestComplete_PartialDelete(t *testing.T) {
	// Deleted relation but nodes are still cached

	cache := ts.cache(t)
	defer cache.Close()
	assertCachedNode(t, cache, 2001)
	assertCachedWay(t, cache, 2001)
	assertCachedWay(t, cache, 2002)

	assertRecords(t, []checkElem{
		{"osm_landusages", -2001, Missing, nil},
		{"osm_landusages", 2001, Missing, nil},
	})
}

func TestComplete_UpdatedNodes(t *testing.T) {
	// Nodes were added, modified or deleted

	c := ts.cache(t)
	defer c.Close()
	if _, err := c.Coords.GetCoord(10000); err != cache.NotFound {
		t.Fatal("coord not missing")
	}

	assertRecords(t, []checkElem{
		{"osm_places", 10001, "village", map[string]string{"name": "Bar"}},
		{"osm_places", 10002, "city", map[string]string{"name": "Baz"}},
	})
}

func TestComplete_LandusageToWaterarea2(t *testing.T) {
	// Parks converted to water moved from landusages to waterareas

	assertRecords(t, []checkElem{
		{"osm_waterareas", 11001, "water", nil},
		{"osm_waterareas", -12001, "water", nil},
		{"osm_waterareas", -13001, "water", nil},

		{"osm_waterareas_gen0", 11001, "water", nil},
		{"osm_waterareas_gen0", -12001, "water", nil},
		{"osm_waterareas_gen0", -13001, "water", nil},

		{"osm_waterareas_gen1", 11001, "water", nil},
		{"osm_waterareas_gen1", -12001, "water", nil},
		{"osm_waterareas_gen1", -13001, "water", nil},

		{"osm_landusages", 11001, Missing, nil},
		{"osm_landusages", -12001, Missing, nil},
		{"osm_landusages", -13001, Missing, nil},

		{"osm_landusages_gen0", 11001, Missing, nil},
		{"osm_landusages_gen0", -12001, Missing, nil},
		{"osm_landusages_gen0", -13001, Missing, nil},

		{"osm_landusages_gen1", 11001, Missing, nil},
		{"osm_landusages_gen1", -12001, Missing, nil},
		{"osm_landusages_gen1", -13001, Missing, nil},
	})
}

func TestComplete_ChangedHoleTags2(t *testing.T) {
	// Newly tagged hole is inserted

	cache := ts.cache(t)
	defer cache.Close()
	assertCachedWay(t, cache, 14001)
	assertCachedWay(t, cache, 14011)

	assertGeomArea(t, checkElem{"osm_waterareas", 14011, "water", nil}, 26672019779)
	assertGeomArea(t, checkElem{"osm_landusages", -14001, "park", nil}, 10373697182)
}

func TestComplete_SplitOuterMultipolygonWay2(t *testing.T) {
	// Splitted outer way of multipolygon was inserted

	diffCache := ts.diffCache(t)
	defer diffCache.Close()
	if ids := diffCache.Ways.Get(15001); len(ids) != 1 || ids[0] != 15001 {
		t.Error("way does not references relation")
	}
	if ids := diffCache.Ways.Get(15002); len(ids) != 1 || ids[0] != 15001 {
		t.Error("way does not references relation")
	}

	assertRecords(t, []checkElem{
		{"osm_landusages", 15001, Missing, nil},
		{"osm_roads", 15002, "residential", nil},
	})
	assertGeomArea(t, checkElem{"osm_landusages", -15001, "park", nil}, 9816216452)
}

func TestComplete_MergeOuterMultipolygonWay2(t *testing.T) {
	// Merged outer way of multipolygon was inserted

	diffCache := ts.diffCache(t)
	defer diffCache.Close()
	if ids := diffCache.Ways.Get(16001); len(ids) != 1 || ids[0] != 16001 {
		t.Error("way does not references relation")
	}
	if ids := diffCache.Ways.Get(16002); len(ids) != 0 {
		t.Error("way references relation")
	}

	cache := ts.cache(t)
	defer cache.Close()
	rel, err := cache.Relations.GetRelation(16001)
	if err != nil {
		t.Fatal(err)
	}
	if len(rel.Members) != 2 || rel.Members[0].Id != 16001 || rel.Members[1].Id != 16011 {
		t.Error("unexpected relation members", rel)
	}

	assertRecords(t, []checkElem{
		{"osm_landusages", 16001, Missing, nil},
		{"osm_roads", 16002, Missing, nil},
	})
	assertGeomArea(t, checkElem{"osm_landusages", -16001, "park", nil}, 12779350582)
}

func TestComplete_NodeWayRefAfterDelete2(t *testing.T) {
	// Node does not referece deleted way

	diffCache := ts.diffCache(t)
	defer diffCache.Close()
	if ids := diffCache.Coords.Get(20001); len(ids) != 0 {
		t.Error("node references way")
	}
	c := ts.cache(t)
	defer c.Close()
	_, err := c.Coords.GetCoord(20002)
	if err != cache.NotFound {
		t.Error("found deleted node")
	}

	assertRecords(t, []checkElem{
		{"osm_roads", 20001, Missing, nil},
		{"osm_barrierpoints", 20001, "block", nil},
	})
}

func TestComplete_WayRelRefAfterDelete2(t *testing.T) {
	// Way does not referece deleted relation

	diffCache := ts.diffCache(t)
	defer diffCache.Close()
	if ids := diffCache.Ways.Get(21001); len(ids) != 0 {
		t.Error("way references relation")
	}

	assertRecords(t, []checkElem{
		{"osm_roads", 21001, "residential", nil},
		{"osm_landusages", 21001, Missing, nil},
		{"osm_landusages", -21001, Missing, nil},
	})
}

func TestComplete_ResidentialToSecondary2(t *testing.T) {
	// New secondary (from residential) is now in roads_gen0/1.

	assertRecords(t, []checkElem{
		{"osm_roads", 40001, "secondary", nil},
		{"osm_roads_gen0", 40001, "secondary", nil},
		{"osm_roads_gen1", 40001, "secondary", nil},
	})
}

func TestComplete_RelationAfterRemove(t *testing.T) {
	// Relation is deleted and way is still present.
	assertRecords(t, []checkElem{
		{"osm_buildings", 50011, "yes", nil},
		{"osm_landusages", 50021, Missing, nil},
		{"osm_landusages", -50021, Missing, nil},
	})
}

func TestComplete_RelationWithoutTags2(t *testing.T) {
	// Relation without tags is removed.

	c := ts.cache(t)
	defer c.Close()
	assertCachedWay(t, c, 50111)

	_, err := c.Ways.GetWay(20002)
	if err != cache.NotFound {
		t.Error("found deleted node")
	}

	assertRecords(t, []checkElem{
		{"osm_buildings", 50111, "yes", nil},
		{"osm_buildings", 50121, Missing, nil},
		{"osm_buildings", -50121, Missing, nil},
	})
}

func TestComplete_DuplicateIds2(t *testing.T) {
	// Only relation/way with same ID was deleted.

	assertRecords(t, []checkElem{
		{"osm_buildings", 51001, "way", nil},
		{"osm_buildings", -51001, Missing, nil},
		{"osm_buildings", 51011, Missing, nil},
		{"osm_buildings", -51011, "mp", nil},
	})
}

func TestComplete_RelationUpdatedByNode2(t *testing.T) {
	// Relations was updated after modified node.

	assertGeomArea(t, checkElem{"osm_buildings", -52121, "yes", nil}, 16276875196.653734)
}

func TestComplete_UpdatedWay2(t *testing.T) {
	// All nodes of straightened way are updated.

	// new length 0.1 degree
	assertGeomLength(t, checkElem{"osm_roads", 60000, "park", nil}, 20037508.342789244/180.0/10.0)
}

func TestComplete_UpdateNodeToCoord2(t *testing.T) {
	// Node is becomes coord after tags are removed.

	assertRecords(t, []checkElem{
		{"osm_amenities", 70001, Missing, nil},
		{"osm_amenities", 70002, "police", nil},
	})
}

func TestComplete_NoDuplicateInsert(t *testing.T) {
	// Relation is not inserted again if a nother relation with the same way was modified
	// Checks #65

	assertRecords(t, []checkElem{
		{"osm_landusages", -201191, "park", nil},
		{"osm_landusages", -201192, "forest", nil},
		{"osm_roads", 201151, "residential", nil},
	})
}

func TestComplete_UnsupportedRelation(t *testing.T) {
	// Unsupported relation type is not inserted with update

	assertRecords(t, []checkElem{
		{"osm_landusages", -201291, Missing, nil},
		{"osm_landusages", 201251, "park", nil},
	})
}

// #######################################################################

func TestComplete_DeployRevert(t *testing.T) {
	if ts.tableExists(t, dbschemaImport, "osm_roads") {
		t.Fatalf("table osm_roads exists in schema %s", dbschemaImport)
	}
	if !ts.tableExists(t, dbschemaProduction, "osm_roads") {
		t.Fatalf("table osm_roads does not exists in schema %s", dbschemaProduction)
	}
	if ts.tableExists(t, dbschemaBackup, "osm_roads") {
		t.Fatalf("table osm_roads exists in schema %s", dbschemaBackup)
	}

	ts.importOsm(t)

	if !ts.tableExists(t, dbschemaImport, "osm_roads") {
		t.Fatalf("table osm_roads does not exists in schema %s", dbschemaImport)
	}
	if !ts.tableExists(t, dbschemaProduction, "osm_roads") {
		t.Fatalf("table osm_roads does not exists in schema %s", dbschemaProduction)
	}
	if ts.tableExists(t, dbschemaBackup, "osm_roads") {
		t.Fatalf("table osm_roads exists in schema %s", dbschemaBackup)
	}

	ts.deployOsm(t)

	if ts.tableExists(t, dbschemaImport, "osm_roads") {
		t.Fatalf("table osm_roads exists in schema %s", dbschemaImport)
	}
	if !ts.tableExists(t, dbschemaProduction, "osm_roads") {
		t.Fatalf("table osm_roads does not exists in schema %s", dbschemaProduction)
	}
	if !ts.tableExists(t, dbschemaBackup, "osm_roads") {
		t.Fatalf("table osm_roads does exists in schema %s", dbschemaBackup)
	}

	ts.revertDeployOsm(t)

	if !ts.tableExists(t, dbschemaImport, "osm_roads") {
		t.Fatalf("table osm_roads does not exists in schema %s", dbschemaImport)
	}
	if !ts.tableExists(t, dbschemaProduction, "osm_roads") {
		t.Fatalf("table osm_roads does not exists in schema %s", dbschemaProduction)
	}
	if ts.tableExists(t, dbschemaBackup, "osm_roads") {
		t.Fatalf("table osm_roads exists in schema %s", dbschemaBackup)
	}
}

func TestComplete_RemoveBackup(t *testing.T) {
	if !ts.tableExists(t, dbschemaImport, "osm_roads") {
		t.Fatalf("table osm_roads does not exists in schema %s", dbschemaImport)
	}
	if !ts.tableExists(t, dbschemaProduction, "osm_roads") {
		t.Fatalf("table osm_roads does not exists in schema %s", dbschemaProduction)
	}
	if ts.tableExists(t, dbschemaBackup, "osm_roads") {
		t.Fatalf("table osm_roads exists in schema %s", dbschemaBackup)
	}

	ts.deployOsm(t)

	if ts.tableExists(t, dbschemaImport, "osm_roads") {
		t.Fatalf("table osm_roads exists in schema %s", dbschemaImport)
	}
	if !ts.tableExists(t, dbschemaProduction, "osm_roads") {
		t.Fatalf("table osm_roads does not exists in schema %s", dbschemaProduction)
	}
	if !ts.tableExists(t, dbschemaBackup, "osm_roads") {
		t.Fatalf("table osm_roads does exists in schema %s", dbschemaBackup)
	}

	ts.removeBackupOsm(t)

	if ts.tableExists(t, dbschemaImport, "osm_roads") {
		t.Fatalf("table osm_roads exists in schema %s", dbschemaImport)
	}
	if !ts.tableExists(t, dbschemaProduction, "osm_roads") {
		t.Fatalf("table osm_roads does not exists in schema %s", dbschemaProduction)
	}
	if ts.tableExists(t, dbschemaBackup, "osm_roads") {
		t.Fatalf("table osm_roads exists in schema %s", dbschemaBackup)
	}
}

func TestComplete_Cleanup(t *testing.T) {
	ts.dropSchemas()
	if err := os.RemoveAll(ts.dir); err != nil {
		t.Error(err)
	}
}
