package test

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/geom/geos"
	"github.com/omniscale/imposm3/proj"
)

func TestComplete(t *testing.T) {
	if testing.Short() {
		t.Skip("system test skipped with -test.short")
	}
	t.Parallel()

	ts := importTestSuite{
		name: "complete",
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
			osmFileName:     "build/complete_db.pbf",
			mappingFileName: "complete_db_mapping.json",
		}
		ts.g = geos.NewGeos()

		ts.db, err = sql.Open("postgres", "sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		ts.dropSchemas()
	})

	t.Run("Import", func(t *testing.T) {
		if ts.tableExists(t, ts.dbschemaImport(), "osm_roads") != false {
			t.Fatalf("table osm_roads exists in schema %s", ts.dbschemaImport())
		}
		ts.importOsm(t)
		if ts.tableExists(t, ts.dbschemaImport(), "osm_roads") != true {
			t.Fatalf("table osm_roads does not exists in schema %s", ts.dbschemaImport())
		}
	})

	t.Run("Deploy", func(t *testing.T) {
		ts.deployOsm(t)
		if ts.tableExists(t, ts.dbschemaImport(), "osm_roads") != false {
			t.Fatalf("table osm_roads exists in schema %s", ts.dbschemaImport())
		}
		if ts.tableExists(t, ts.dbschemaProduction(), "osm_roads") != true {
			t.Fatalf("table osm_roads does not exists in schema %s", ts.dbschemaProduction())
		}
	})

	t.Run("CheckIndices", func(t *testing.T) {
		if !ts.indexExists(t, ts.dbschemaProduction(), "osm_roads", "osm_roads_pkey") {
			t.Fatal("osm_id idx missing for osm_roads")
		}
		if !ts.indexExists(t, ts.dbschemaProduction(), "osm_roads", "osm_roads_geom") {
			t.Fatal("geom idx missing for osm_roads")
		}
		if !ts.indexExists(t, ts.dbschemaProduction(), "osm_landusages_gen0", "osm_landusages_gen0_osm_id_idx") {
			t.Fatal("osm_id idx missing for osm_landusages_gen0")
		}
		if !ts.indexExists(t, ts.dbschemaProduction(), "osm_landusages_gen0", "osm_landusages_gen0_geom") {
			t.Fatal("geom idx missing for osm_landusages_gen0")
		}

	})

	t.Run("OnlyNewStyleMultipolgon", func(t *testing.T) {
		ts.assertRecords(t, []checkElem{
			{"osm_landusages", -1001, "wood", nil},
			{"osm_landusages", -1011, Missing, nil},
			{"osm_landusages", -1021, Missing, nil},
		})
	})

	t.Run("LandusageToWaterarea1", func(t *testing.T) {
		// Parks inserted into landusages
		cache := ts.cache(t)
		defer cache.Close()
		ts.assertCachedWay(t, cache, 11001)
		ts.assertCachedWay(t, cache, 13001)

		ts.assertRecords(t, []checkElem{
			{"osm_waterareas", 11001, Missing, nil},
			{"osm_waterareas", -13001, Missing, nil},

			{"osm_waterareas_gen0", 11001, Missing, nil},
			{"osm_waterareas_gen0", -13001, Missing, nil},

			{"osm_waterareas_gen1", 11001, Missing, nil},
			{"osm_waterareas_gen1", -13001, Missing, nil},

			{"osm_landusages", 11001, "park", nil},
			{"osm_landusages", -13001, "park", nil},

			{"osm_landusages_gen0", 11001, "park", nil},
			{"osm_landusages_gen0", -13001, "park", nil},

			{"osm_landusages_gen1", 11001, "park", nil},
			{"osm_landusages_gen1", -13001, "park", nil},
		})
	})

	t.Run("ChangedHoleTags1", func(t *testing.T) {
		// Multipolygon relation with untagged hole
		cache := ts.cache(t)
		defer cache.Close()
		ts.assertCachedWay(t, cache, 14001)
		ts.assertCachedWay(t, cache, 14011)

		ts.assertRecords(t, []checkElem{
			{"osm_waterareas", 14011, Missing, nil},
			{"osm_waterareas", -14011, Missing, nil},
			{"osm_landusages", 14001, "park", nil},
			{"osm_landusages", -14001, Missing, nil},
		})
	})

	t.Run("SplitOuterMultipolygonWay1", func(t *testing.T) {
		// Single outer way of multipolygon was inserted.
		ts.assertRecords(t, []checkElem{
			{"osm_roads", 15002, Missing, nil},
			{"osm_landusages", -15001, "park", nil},
		})
		ts.assertGeomArea(t, checkElem{"osm_landusages", -15001, "park", nil}, 9816216452)
	})

	t.Run("MergeOuterMultipolygonWay1", func(t *testing.T) {
		// Splitted outer way of multipolygon was inserted.
		ts.assertRecords(t, []checkElem{
			{"osm_landusages", -16001, "park", nil},
			{"osm_roads", 16002, "residential", nil},
		})
		ts.assertGeomArea(t, checkElem{"osm_landusages", -16001, "park", nil}, 12779350582)
	})

	t.Run("BrokenMultipolygonWays", func(t *testing.T) {
		// MultiPolygons with broken outer ways are handled.
		// outer way does not merge (17002 has one node)
		ts.assertRecords(t, []checkElem{
			{"osm_landusages", -17001, Missing, nil},
			{"osm_roads", 17001, "residential", nil},
			{"osm_roads", 17002, Missing, nil},
		})

		// outer way does not merge (17102 has no nodes)
		ts.assertRecords(t, []checkElem{
			{"osm_landusages", -17101, Missing, nil},
			{"osm_roads", 17101, "residential", nil},
			{"osm_roads", 17102, Missing, nil},
		})
	})

	t.Run("WayWithInvalidLayer", func(t *testing.T) {
		// Layer value is not a valid int32.
		ts.assertRecords(t, []checkElem{
			{"osm_roads", 17003, "residential", map[string]string{"z_order": "NULL"}},
		})
	})

	t.Run("NodeWayInsertedTwice", func(t *testing.T) {
		// Way with multiple mappings is inserted twice in same table
		rows := ts.queryRows(t, "osm_roads", 18001)
		if len(rows) != 2 || rows[0].osmType != "residential" || rows[1].osmType != "tram" {
			t.Errorf("unexpected roads: %v", rows)
		}
	})

	t.Run("OuterWayInsertedTwice", func(t *testing.T) {
		// Outer way with different tag value is inserted twice into same table
		// behavior changed from pre-old-style-mp-removal:
		//    test outer way not inserted (different tag but same table)
		ts.assertRecords(t, []checkElem{
			{"osm_landusages", -19001, "farmland", nil},
			{"osm_landusages", 19002, "farmyard", nil},
			{"osm_landusages", 19001, "farm", nil},
		})
	})

	t.Run("OuterWayInserted", func(t *testing.T) {
		// Outer way with different tag is inserted twice into different table
		ts.assertRecords(t, []checkElem{
			{"osm_landusages", 19101, "farm", nil},
			{"osm_landusages", 19102, "farmyard", nil},
			{"osm_admin", -19101, "administrative", nil},
		})
	})

	t.Run("NodeWayRefAfterDelete1", func(t *testing.T) {
		// Nodes references way

		cache := ts.diffCache(t)
		defer cache.Close()
		if ids := cache.Coords.Get(20001); len(ids) != 1 || ids[0] != 20001 {
			t.Error("node does not references way")
		}
		if ids := cache.Coords.Get(20002); len(ids) != 1 || ids[0] != 20001 {
			t.Error("node does not references way")
		}

		ts.assertRecords(t, []checkElem{
			{"osm_roads", 20001, "residential", nil},
			{"osm_barrierpoints", 20001, "block", nil},
		})
	})

	t.Run("WayRelRefAfterDelete1", func(t *testing.T) {
		// Ways references relation

		cache := ts.diffCache(t)
		defer cache.Close()
		if ids := cache.Ways.Get(21001); len(ids) != 1 || ids[0] != 21001 {
			t.Error("way does not references relation")
		}

		ts.assertRecords(t, []checkElem{
			{"osm_roads", 21001, "residential", nil},
			{"osm_landusages", -21001, "park", nil},
		})
	})

	t.Run("OldStyleMpRelationWayInserted", func(t *testing.T) {
		// Old-style-mp: Part of relation is now inserted.
		ts.assertRecords(t, []checkElem{
			{"osm_landusages", -9001, "park", map[string]string{"name": "rel 9001"}},
			{"osm_landusages", 9009, "park", map[string]string{"name": "way 9009"}},
			{"osm_landusages", -9101, "park", map[string]string{"name": "rel 9101"}},
			{"osm_landusages", 9109, "park", map[string]string{"name": "way 9109"}},
			{"osm_landusages", 9110, "scrub", nil},
		})
	})

	t.Run("RelationWaysInserted", func(t *testing.T) {
		// Outer ways of multipolygon are inserted.
		ts.assertRecords(t, []checkElem{
			// no name on relation
			{"osm_landusages", -9201, "park", map[string]string{"name": ""}},
			{"osm_landusages", 9201, Missing, nil},
			{"osm_landusages", 9209, Missing, nil},
			{"osm_landusages", 9210, Missing, nil},
			// outer ways of multipolygon stand for their own
			{"osm_roads", 9209, "secondary", map[string]string{"name": "9209"}},
			{"osm_roads", 9210, "residential", map[string]string{"name": "9210"}},
		})
	})

	t.Run("RelationWayInserted", func(t *testing.T) {
		// Part of relation was inserted twice.
		ts.assertRecords(t, []checkElem{
			{"osm_landusages", -8001, "park", map[string]string{"name": "rel 8001"}},
			{"osm_roads", 8009, "residential", nil},
		})
	})

	t.Run("SingleNodeWaysNotInserted", func(t *testing.T) {
		// Ways with single/duplicate nodes are not inserted.
		ts.assertRecords(t, []checkElem{
			{"osm_landusages", 30001, Missing, nil},
			{"osm_landusages", 30002, Missing, nil},
			{"osm_landusages", 30003, Missing, nil},
		})
	})

	t.Run("PolygonWithDuplicateNodesIsValid", func(t *testing.T) {
		// Polygon with duplicate nodes is valid.
		ts.assertGeomValid(t, checkElem{"osm_landusages", 30005, "park", nil})
	})

	t.Run("IncompletePolygons", func(t *testing.T) {
		// Non-closed/incomplete polygons are not inserted.

		ts.assertRecords(t, []checkElem{
			{"osm_landusages", 30004, Missing, nil},
			{"osm_landusages", 30006, Missing, nil},
		})
	})

	t.Run("ResidentialToSecondary", func(t *testing.T) {
		// Residential road is not in roads_gen0/1.

		ts.assertRecords(t, []checkElem{
			{"osm_roads", 40001, "residential", nil},
			{"osm_roads_gen0", 40001, Missing, nil},
			{"osm_roads_gen1", 40002, Missing, nil},
		})
	})

	t.Run("RelationBeforeRemove", func(t *testing.T) {
		// Relation and way is inserted.

		ts.assertRecords(t, []checkElem{
			{"osm_buildings", 50011, "yes", nil},
			{"osm_landusages", -50021, "park", nil},
		})
	})

	t.Run("OldStyleRelationIsIgnored", func(t *testing.T) {
		// Relation without tags is not inserted.

		ts.assertRecords(t, []checkElem{
			{"osm_buildings", 50111, "yes", nil},
			{"osm_buildings", -50121, Missing, nil},
		})
	})

	t.Run("DuplicateIDs", func(t *testing.T) {
		// Relation/way with same ID is inserted.

		ts.assertRecords(t, []checkElem{
			{"osm_buildings", 51001, "way", nil},
			{"osm_buildings", -51001, "mp", nil},
			{"osm_buildings", 51011, "way", nil},
			{"osm_buildings", -51011, "mp", nil},
		})
	})

	t.Run("RelationUpdatedByNode", func(t *testing.T) {
		// Relations was updated after modified node.

		ts.assertGeomArea(t, checkElem{"osm_buildings", -52121, "yes", nil}, 13653930440.868315)
	})

	t.Run("GeneralizedBananaPolygonIsValid", func(t *testing.T) {
		// Generalized polygons are valid.

		ts.assertGeomValid(t, checkElem{"osm_landusages", 7101, Missing, nil})
		// simplified geometies are valid too
		ts.assertGeomValid(t, checkElem{"osm_landusages_gen0", 7101, Missing, nil})
		ts.assertGeomValid(t, checkElem{"osm_landusages_gen1", 7101, Missing, nil})
	})

	t.Run("GeneralizedLinestringIsValid", func(t *testing.T) {
		// Generalized linestring is valid.

		// geometry is not simple, but valid
		ts.assertGeomLength(t, checkElem{"osm_roads", 7201, "primary", nil}, 1243660.044819)
		if ts.g.IsSimple(ts.queryGeom(t, "osm_roads", 7201)) {
			t.Errorf("expected non-simple geometry for 7201")
		}
		// check that geometry 'survives' simplification
		ts.assertGeomLength(t, checkElem{"osm_roads_gen0", 7201, "primary", nil}, 1243660.044819)
		ts.assertGeomLength(t, checkElem{"osm_roads_gen1", 7201, "primary", nil}, 1243660.044819)
	})

	t.Run("RingWithGap", func(t *testing.T) {
		// Multipolygon with gap (overlapping but different endpoints) gets closed
		ts.assertGeomValid(t, checkElem{"osm_landusages", -7301, Missing, nil})
		// but not way
		ts.assertRecords(t, []checkElem{
			checkElem{"osm_landusages", 7311, Missing, nil},
		})
	})

	t.Run("MultipolygonWithOpenRing", func(t *testing.T) {
		// Multipolygon is inserted even if there is an open ring/member
		ts.assertGeomValid(t, checkElem{"osm_landusages", -7401, Missing, nil})
	})

	t.Run("UpdatedNodes1", func(t *testing.T) {
		// Zig-Zag line is inserted.
		ts.assertGeomLength(t, checkElem{"osm_roads", 60000, Missing, nil}, 14035.61150207768)
	})

	t.Run("UpdateNodeToCoord1", func(t *testing.T) {
		// Node is inserted with tag.
		ts.assertRecords(t, []checkElem{
			{"osm_amenities", 70001, "police", nil},
			{"osm_amenities", 70002, Missing, nil},
		})
	})

	t.Run("EnumerateKey", func(t *testing.T) {
		// Enumerate from key.
		ts.assertRecords(t, []checkElem{
			{"osm_landusages", 100001, "park", map[string]string{"enum": "1"}},
			{"osm_landusages", 100002, "park", map[string]string{"enum": "0"}},
			{"osm_landusages", 100003, "wood", map[string]string{"enum": "15"}},
		})
	})

	t.Run("AreaMapping", func(t *testing.T) {
		// Mapping type dependent area-defaults.

		ts.assertRecords(t, []checkElem{
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
	})

	t.Run("HstoreTags", func(t *testing.T) {
		// Mapping type dependent area-defaults.

		ts.assertHstore(t, []checkElem{
			{"osm_buildings", 401151, "*", map[string]string{"amenity": "fuel", "opening_hours": "24/7"}},
		})
	})

	// #######################################################################

	t.Run("Update", func(t *testing.T) {
		ts.updateOsm(t, "build/complete_db.osc.gz")
	})

	// #######################################################################

	t.Run("NoDuplicates", func(t *testing.T) {
		// Relations/ways are only inserted once Checks #66

		for _, table := range []string{"osm_roads", "osm_landusages"} {
			rows, err := ts.db.Query(
				fmt.Sprintf(`SELECT osm_id, count(osm_id) FROM "%s"."%s" GROUP BY osm_id HAVING count(osm_id) > 1`,
					ts.dbschemaProduction(), table))
			if err != nil {
				t.Fatal(err)
			}
			var osmID, count int64
			for rows.Next() {
				if err := rows.Scan(&osmID, &count); err != nil {
					t.Fatal(err)
				}
				if table == "osm_roads" && osmID == 18001 {
					// # duplicate for TestNodeWayInsertedTwice is expected
					if count != 2 {
						t.Error("highway not inserted twice", osmID, count)
					}
				} else {
					t.Error("found duplicate way in osm_roads", osmID, count)
				}
			}
		}
	})

	t.Run("UpdatedLandusage", func(t *testing.T) {
		// Multipolygon relation was modified

		nd := osm.Node{Long: 13.4, Lat: 47.5}
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
	})

	t.Run("PartialDelete", func(t *testing.T) {
		// Deleted relation but nodes are still cached

		cache := ts.cache(t)
		defer cache.Close()
		ts.assertCachedNode(t, cache, 2001)
		ts.assertCachedWay(t, cache, 2001)
		ts.assertCachedWay(t, cache, 2002)

		ts.assertRecords(t, []checkElem{
			{"osm_landusages", -2001, Missing, nil},
			{"osm_landusages", 2001, Missing, nil},
		})
	})

	t.Run("UpdatedNodes", func(t *testing.T) {
		// Nodes were added, modified or deleted

		c := ts.cache(t)
		defer c.Close()
		if _, err := c.Coords.GetCoord(10000); err != cache.NotFound {
			t.Fatal("coord not missing")
		}

		ts.assertRecords(t, []checkElem{
			{"osm_places", 10001, "village", map[string]string{"name": "Bar"}},
			{"osm_places", 10002, "city", map[string]string{"name": "Baz"}},
		})
	})

	t.Run("LandusageToWaterarea2", func(t *testing.T) {
		// Parks converted to water moved from landusages to waterareas

		ts.assertRecords(t, []checkElem{
			{"osm_waterareas", 11001, "water", nil},
			{"osm_waterareas", -13001, "water", nil},

			{"osm_waterareas_gen0", 11001, "water", nil},
			{"osm_waterareas_gen0", -13001, "water", nil},

			{"osm_waterareas_gen1", 11001, "water", nil},
			{"osm_waterareas_gen1", -13001, "water", nil},

			{"osm_landusages", 11001, Missing, nil},
			{"osm_landusages", -13001, Missing, nil},

			{"osm_landusages_gen0", 11001, Missing, nil},
			{"osm_landusages_gen0", -13001, Missing, nil},

			{"osm_landusages_gen1", 11001, Missing, nil},
			{"osm_landusages_gen1", -13001, Missing, nil},
		})
	})

	t.Run("ChangedHoleTags2", func(t *testing.T) {
		// Newly tagged hole is inserted

		cache := ts.cache(t)
		defer cache.Close()
		ts.assertCachedWay(t, cache, 14001)
		ts.assertCachedWay(t, cache, 14011)

		ts.assertGeomArea(t, checkElem{"osm_waterareas", 14011, "water", nil}, 26672019779)
		ts.assertGeomArea(t, checkElem{"osm_landusages", -14001, "park", nil}, 10373697182)

		ts.assertRecords(t, []checkElem{
			{"osm_waterareas", -14011, Missing, nil},
			{"osm_landusages", -14001, "park", nil},
		})
	})

	t.Run("SplitOuterMultipolygonWay2", func(t *testing.T) {
		// Splitted outer way of multipolygon was inserted

		diffCache := ts.diffCache(t)
		defer diffCache.Close()
		if ids := diffCache.Ways.Get(15001); len(ids) != 1 || ids[0] != 15001 {
			t.Error("way does not references relation")
		}
		if ids := diffCache.Ways.Get(15002); len(ids) != 1 || ids[0] != 15001 {
			t.Error("way does not references relation")
		}

		ts.assertRecords(t, []checkElem{
			{"osm_landusages", 15001, Missing, nil},
			{"osm_roads", 15002, "residential", nil},
		})
		ts.assertGeomArea(t, checkElem{"osm_landusages", -15001, "park", nil}, 9816216452)
	})

	t.Run("MergeOuterMultipolygonWay2", func(t *testing.T) {
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
		if len(rel.Members) != 2 || rel.Members[0].ID != 16001 || rel.Members[1].ID != 16011 {
			t.Error("unexpected relation members", rel)
		}

		ts.assertRecords(t, []checkElem{
			{"osm_landusages", 16001, Missing, nil},
			{"osm_roads", 16002, Missing, nil},
		})
		ts.assertGeomArea(t, checkElem{"osm_landusages", -16001, "park", nil}, 12779350582)
	})

	t.Run("WayWithInvalidLayerUpdate", func(t *testing.T) {
		// Layer value is now a valid int32.
		ts.assertRecords(t, []checkElem{
			{"osm_roads", 17003, "residential", map[string]string{"z_order": "23"}},
		})
	})

	t.Run("NodeWayRefAfterDelete2", func(t *testing.T) {
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

		ts.assertRecords(t, []checkElem{
			{"osm_roads", 20001, Missing, nil},
			{"osm_barrierpoints", 20001, "block", nil},
		})
	})

	t.Run("WayRelRefAfterDelete2", func(t *testing.T) {
		// Way does not referece deleted relation

		diffCache := ts.diffCache(t)
		defer diffCache.Close()
		if ids := diffCache.Ways.Get(21001); len(ids) != 0 {
			t.Error("way references relation")
		}

		ts.assertRecords(t, []checkElem{
			{"osm_roads", 21001, "residential", nil},
			{"osm_landusages", 21001, Missing, nil},
			{"osm_landusages", -21001, Missing, nil},
		})
	})

	t.Run("ResidentialToSecondary2", func(t *testing.T) {
		// New secondary (from residential) is now in roads_gen0/1.

		ts.assertRecords(t, []checkElem{
			{"osm_roads", 40001, "secondary", nil},
			{"osm_roads_gen0", 40001, "secondary", nil},
			{"osm_roads_gen1", 40001, "secondary", nil},
		})
	})

	t.Run("RelationAfterRemove", func(t *testing.T) {
		// Relation is deleted and way is still present.
		ts.assertRecords(t, []checkElem{
			{"osm_buildings", 50011, "yes", nil},
			{"osm_landusages", 50021, Missing, nil},
			{"osm_landusages", -50021, Missing, nil},
		})
	})

	t.Run("RelationWithoutTags2", func(t *testing.T) {
		// Relation without tags is removed.

		c := ts.cache(t)
		defer c.Close()
		ts.assertCachedWay(t, c, 50111)

		_, err := c.Ways.GetWay(20002)
		if err != cache.NotFound {
			t.Error("found deleted node")
		}

		ts.assertRecords(t, []checkElem{
			{"osm_buildings", 50111, "yes", nil},
			{"osm_buildings", 50121, Missing, nil},
			{"osm_buildings", -50121, Missing, nil},
		})
	})

	t.Run("DuplicateIDs2", func(t *testing.T) {
		// Only relation/way with same ID was deleted.

		ts.assertRecords(t, []checkElem{
			{"osm_buildings", 51001, "way", nil},
			{"osm_buildings", -51001, Missing, nil},
			{"osm_buildings", 51011, Missing, nil},
			{"osm_buildings", -51011, "mp", nil},
		})
	})

	t.Run("RelationUpdatedByNode2", func(t *testing.T) {
		// Relations was updated after modified node.

		ts.assertGeomArea(t, checkElem{"osm_buildings", -52121, "yes", nil}, 16276875196.653734)
	})

	t.Run("DuplicateNodeCreate", func(t *testing.T) {
		// Duplicate 'create node' does not result in duplicate way.

		ts.assertRecords(t, []checkElem{
			{"osm_buildings", 53111, "way", nil},
		})
	})

	t.Run("UpdatedWay2", func(t *testing.T) {
		// All nodes of straightened way are updated.

		// new length 0.1 degree
		ts.assertGeomLength(t, checkElem{"osm_roads", 60000, "park", nil}, 20037508.342789244/180.0/10.0)
	})

	t.Run("UpdateNodeToCoord2", func(t *testing.T) {
		// Node is becomes coord after tags are removed.

		ts.assertRecords(t, []checkElem{
			{"osm_amenities", 70001, Missing, nil},
			{"osm_amenities", 70002, "police", nil},
		})
	})

	t.Run("NoDuplicateInsert", func(t *testing.T) {
		// Relation is not inserted again if a nother relation with the same way was modified
		// Checks #65

		ts.assertRecords(t, []checkElem{
			{"osm_landusages", -201191, "park", nil},
			{"osm_landusages", -201192, "forest", nil},
			{"osm_roads", 201151, "residential", nil},
		})
	})

	t.Run("UnsupportedRelation", func(t *testing.T) {
		// Unsupported relation type is not inserted with update

		ts.assertRecords(t, []checkElem{
			{"osm_landusages", -201291, Missing, nil},
			{"osm_landusages", 201251, "park", nil},
		})
	})

	// #######################################################################

	t.Run("DeployRevert", func(t *testing.T) {
		if ts.tableExists(t, ts.dbschemaImport(), "osm_roads") {
			t.Fatalf("table osm_roads exists in schema %s", ts.dbschemaImport())
		}
		if !ts.tableExists(t, ts.dbschemaProduction(), "osm_roads") {
			t.Fatalf("table osm_roads does not exists in schema %s", ts.dbschemaProduction())
		}
		if ts.tableExists(t, ts.dbschemaBackup(), "osm_roads") {
			t.Fatalf("table osm_roads exists in schema %s", ts.dbschemaBackup())
		}

		ts.importOsm(t)

		if !ts.tableExists(t, ts.dbschemaImport(), "osm_roads") {
			t.Fatalf("table osm_roads does not exists in schema %s", ts.dbschemaImport())
		}
		if !ts.tableExists(t, ts.dbschemaProduction(), "osm_roads") {
			t.Fatalf("table osm_roads does not exists in schema %s", ts.dbschemaProduction())
		}
		if ts.tableExists(t, ts.dbschemaBackup(), "osm_roads") {
			t.Fatalf("table osm_roads exists in schema %s", ts.dbschemaBackup())
		}

		ts.deployOsm(t)

		if ts.tableExists(t, ts.dbschemaImport(), "osm_roads") {
			t.Fatalf("table osm_roads exists in schema %s", ts.dbschemaImport())
		}
		if !ts.tableExists(t, ts.dbschemaProduction(), "osm_roads") {
			t.Fatalf("table osm_roads does not exists in schema %s", ts.dbschemaProduction())
		}
		if !ts.tableExists(t, ts.dbschemaBackup(), "osm_roads") {
			t.Fatalf("table osm_roads does exists in schema %s", ts.dbschemaBackup())
		}

		ts.revertDeployOsm(t)

		if !ts.tableExists(t, ts.dbschemaImport(), "osm_roads") {
			t.Fatalf("table osm_roads does not exists in schema %s", ts.dbschemaImport())
		}
		if !ts.tableExists(t, ts.dbschemaProduction(), "osm_roads") {
			t.Fatalf("table osm_roads does not exists in schema %s", ts.dbschemaProduction())
		}
		if ts.tableExists(t, ts.dbschemaBackup(), "osm_roads") {
			t.Fatalf("table osm_roads exists in schema %s", ts.dbschemaBackup())
		}
	})

	t.Run("RemoveBackup", func(t *testing.T) {
		if !ts.tableExists(t, ts.dbschemaImport(), "osm_roads") {
			t.Fatalf("table osm_roads does not exists in schema %s", ts.dbschemaImport())
		}
		if !ts.tableExists(t, ts.dbschemaProduction(), "osm_roads") {
			t.Fatalf("table osm_roads does not exists in schema %s", ts.dbschemaProduction())
		}
		if ts.tableExists(t, ts.dbschemaBackup(), "osm_roads") {
			t.Fatalf("table osm_roads exists in schema %s", ts.dbschemaBackup())
		}

		ts.deployOsm(t)

		if ts.tableExists(t, ts.dbschemaImport(), "osm_roads") {
			t.Fatalf("table osm_roads exists in schema %s", ts.dbschemaImport())
		}
		if !ts.tableExists(t, ts.dbschemaProduction(), "osm_roads") {
			t.Fatalf("table osm_roads does not exists in schema %s", ts.dbschemaProduction())
		}
		if !ts.tableExists(t, ts.dbschemaBackup(), "osm_roads") {
			t.Fatalf("table osm_roads does exists in schema %s", ts.dbschemaBackup())
		}

		ts.removeBackupOsm(t)

		if ts.tableExists(t, ts.dbschemaImport(), "osm_roads") {
			t.Fatalf("table osm_roads exists in schema %s", ts.dbschemaImport())
		}
		if !ts.tableExists(t, ts.dbschemaProduction(), "osm_roads") {
			t.Fatalf("table osm_roads does not exists in schema %s", ts.dbschemaProduction())
		}
		if ts.tableExists(t, ts.dbschemaBackup(), "osm_roads") {
			t.Fatalf("table osm_roads exists in schema %s", ts.dbschemaBackup())
		}
	})

	t.Run("Cleanup", func(t *testing.T) {
		ts.dropSchemas()
		if err := os.RemoveAll(ts.dir); err != nil {
			t.Error(err)
		}
	})
}
