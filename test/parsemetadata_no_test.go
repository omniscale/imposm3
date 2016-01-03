package test

/*
Test case:
    "tags": {
        "load_all": true
    },
    "use_single_id_space": true,

Expected:
* Default compatibility:  Drop single "created_by" data  ( keep_single_createdby_tag": false )
* Default compatibility:  No metadata!
* Not overwrite special tags like ("_version_","_timestamp_",... )


Test command with verbose logging:
      godep go test ./test/helper_test.go  ./test/parsemetadata_no_test.go -v

TODO:  cache test
*/

import (
	"database/sql"
	"testing"

	"github.com/omniscale/imposm3/geom/geos"
)

func TestParseMetadata_no(t *testing.T) {

	ts.dir = "/tmp/imposm3test_parsemeta_no"
	ts.config = importConfig{
		connection:      "postgis://",
		cacheDir:        ts.dir,
		osmFileName:     "build/parsemetadata_data.pbf",
		mappingFileName: "parsemetadata_no_mapping.json",
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
	if ts.tableExists(t, dbschemaImport, "osm_parsemetadata_no") != false {
		t.Fatalf("table osm_parsemetadata_no exists in schema %s", dbschemaImport)
	}
	ts.importOsm(t)
	if ts.tableExists(t, dbschemaImport, "osm_parsemetadata_no") != true {
		t.Fatalf("table osm_parsemetadata_no does not exists in schema %s", dbschemaImport)
	}

	// =======================================================================
	t.Log("Deploy - step ")
	ts.deployOsm(t)
	if ts.tableExists(t, dbschemaImport, "osm_parsemetadata_no") != false {
		t.Fatalf("table osm_parsemetadata_no exists in schema %s", dbschemaImport)
	}
	if ts.tableExists(t, dbschemaProduction, "osm_parsemetadata_no") != true {
		t.Fatalf("table osm_parsemetadata_no does not exists in schema %s", dbschemaProduction)
	}

	// =======================================================================
	t.Log("Check - step  ")

	assertHstore(t, []checkElem{
		// Nodes
		{"osm_parsemetadata_no", 31001, Missing, nil},
		{"osm_parsemetadata_no", 31002, Missing, nil},
		{"osm_parsemetadata_no", 31003, Missing, nil},
		{"osm_parsemetadata_no", 31004, Missing, nil},
		{"osm_parsemetadata_no", 31101, "tags", map[string]string{
			"amenity":            "cafe",
			"created_by":         "iDEditor",
			"testnote_version":   "11",
			"testnote_timestamp": "2011-11-11T01:11:11Z",
			"testnote_changeset": "3000001",
			"testnote_uid":       "301",
			"testnote_user":      "node301",
		}},
		// Ways
		{"osm_parsemetadata_no", -31002, "tags", map[string]string{
			"barrier":            "fence",
			"testnote_version":   "21",
			"testnote_timestamp": "2011-11-11T02:22:22Z",
			"testnote_changeset": "3000002",
			"testnote_uid":       "302",
			"testnote_user":      "way302",
		}},
		{"osm_parsemetadata_no", -31003, Missing, nil},
		{"osm_parsemetadata_no", -31101, "tags", map[string]string{
			"highway":            "secondary",
			"testnote_version":   "21",
			"testnote_timestamp": "2011-11-11T02:22:22Z",
			"testnote_changeset": "3000002",
			"testnote_uid":       "302",
			"testnote_user":      "way302",
		}},
		// Relations:
		{"osm_parsemetadata_no", RelOffset - 31101, "tags", map[string]string{
			// "type":               "multipolygon",
			"building":           "yes",
			"testnote_version":   "31",
			"testnote_timestamp": "2011-11-11T03:33:33Z",
			"testnote_changeset": "3000003",
			"testnote_uid":       "303",
			"testnote_user":      "rel303",
		}},

		// overwrite test  - node1
		{"osm_parsemetadata_no", 1, "tags", map[string]string{
			"amenity":     "cafe",
			"_version_":   "OpenStreetMap_node_osm_version",
			"_timestamp_": "OpenStreetMap_node_osm_timestamp",
			"_changeset_": "OpenStreetMap_node_osm_changeset",
			"_uid_":       "OpenStreetMap_node_osm_uid",
			"_user_":      "OpenStreetMap_node_osm_user",
		}},

		// overwrite test  - way1
		{"osm_parsemetadata_no", -1, "tags", map[string]string{
			"highway":     "secondary",
			"_version_":   "OpenStreetMap_way_osm_version",
			"_timestamp_": "OpenStreetMap_way_osm_timestamp",
			"_changeset_": "OpenStreetMap_way_osm_changeset",
			"_uid_":       "OpenStreetMap_way_osm_uid",
			"_user_":      "OpenStreetMap_way_osm_user",
		}},
		// overwrite test  - rel1
		{"osm_parsemetadata_no", RelOffset - 1, "tags", map[string]string{
			"amenity":     "cafe",
			"_version_":   "OpenStreetMap_rel_osm_version",
			"_timestamp_": "OpenStreetMap_rel_osm_timestamp",
			"_changeset_": "OpenStreetMap_rel_osm_changeset",
			"_uid_":       "OpenStreetMap_rel_osm_uid",
			"_user_":      "OpenStreetMap_rel_osm_user",
		}},
	})

	// =======================================================================
	t.Log("Update with osc")
	ts.updateOsm(t, "./build/parsemetadata_data.osc.gz")

	// =======================================================================
	t.Log("Check osc ")
	assertHstore(t, []checkElem{
		// Nodes
		{"osm_parsemetadata_no", 31001, Missing, nil},
		{"osm_parsemetadata_no", 31002, Missing, nil},
		{"osm_parsemetadata_no", 31003, Missing, nil},
		{"osm_parsemetadata_no", 31004, Missing, nil},
		//
		{"osm_parsemetadata_no", 31101, "tags", map[string]string{
			"amenity":            "restaurant",
			"created_by":         "JOSM",
			"testnote_version":   "12",
			"testnote_timestamp": "2012-12-22T00:22:11Z",
			"testnote_changeset": "4000001",
			"testnote_uid":       "311",
			"testnote_user":      "node311",
		}},
		// Ways
		{"osm_parsemetadata_no", -31002, "tags", map[string]string{
			"barrier":            "fence",
			"access":             "private", // new
			"testnote_version":   "22",
			"testnote_timestamp": "2012-12-22T00:22:22Z",
			"testnote_changeset": "4000002",
			"testnote_uid":       "312",
			"testnote_user":      "way312",
		}},
		{"osm_parsemetadata_no", -31003, Missing, nil},
		{"osm_parsemetadata_no", -31101, "tags", map[string]string{
			"highway":            "secondary",
			"landuse":            "park", // new
			"testnote_version":   "22",
			"testnote_timestamp": "2012-12-22T00:22:22Z",
			"testnote_changeset": "4000002",
			"testnote_uid":       "312",
			"testnote_user":      "way312",
		}},
		// Relations:
		{"osm_parsemetadata_no", RelOffset - 31101, "tags", map[string]string{
			// "type":            "multipolygon",       --  not added !
			"building":           "yes",
			"amenity":            "pub",                  // new
			"testnote_version":   "32",                   // "31"
			"testnote_timestamp": "2012-12-22T00:22:33Z", // "2011-11-11T03:33:33Z",
			"testnote_changeset": "4000003",              // "3000003",
			"testnote_uid":       "313",                  // "303",
			"testnote_user":      "rel313",               // "rel303",
		}},

		// overwrite test   node1
		{"osm_parsemetadata_no", 1, "tags", map[string]string{
			"amenity":     "cafe",
			"_version_":   "OpenStreetMap_node_osm_version",
			"_timestamp_": "OpenStreetMap_node_osm_timestamp",
			"_changeset_": "OpenStreetMap_node_osm_changeset",
			"_uid_":       "OpenStreetMap_node_osm_uid",
			"_user_":      "OpenStreetMap_node_osm_user",
		}},
		// overwrite test   way1
		{"osm_parsemetadata_no", -1, "tags", map[string]string{
			"highway":     "secondary",
			"_version_":   "OpenStreetMap_way_osm_version",
			"_timestamp_": "OpenStreetMap_way_osm_timestamp",
			"_changeset_": "OpenStreetMap_way_osm_changeset",
			"_uid_":       "OpenStreetMap_way_osm_uid",
			"_user_":      "OpenStreetMap_way_osm_user",
		}},
		// overwrite test   rel1
		{"osm_parsemetadata_no", RelOffset - 1, "tags", map[string]string{
			"amenity":     "cafe",
			"_version_":   "OpenStreetMap_rel_osm_version",
			"_timestamp_": "OpenStreetMap_rel_osm_timestamp",
			"_changeset_": "OpenStreetMap_rel_osm_changeset",
			"_uid_":       "OpenStreetMap_rel_osm_uid",
			"_user_":      "OpenStreetMap_rel_osm_user",
		}},
	})

	t.Log("-- end -- ")
}
