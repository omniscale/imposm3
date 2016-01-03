package test

/*
Test case:
    "tags": {
        "load_all": true,
        "keep_single_createdby_tag": true,
        "parsemetadata": {
	          "create_tag_from_version": "_version_",
	          "create_tag_from_timestamp": "_timestamp_",
	          "create_tag_from_changeset": "_changeset_",
	          "create_tag_from_uid": "_uid_",
	          "create_tag_from_user": "_user_"
        }
    },
    "use_single_id_space": true,

Expected:
* KEEP single "created_by" data  ( keep_single_createdby_tag": true )
* Add 5 metadata tags:  ( "_version_",  "_timestamp_", "_changeset_", "_uid_", "_user_",  )
* Overwrite special tags:  ( "_version_",  "_timestamp_", "_changeset_", "_uid_", "_user_",  )


Test command with verbose logging:
     godep go test ./test/helper_test.go  ./test/parsemetadata_yes_test.go -v

TODO:  cache test
*/

import (
	"database/sql"
	"testing"

	"github.com/omniscale/imposm3/geom/geos"
)

func TestParseMetadata_yes(t *testing.T) {

	ts.dir = "/tmp/imposm3test_parsemeta_yes"
	ts.config = importConfig{
		connection:      "postgis://",
		cacheDir:        ts.dir,
		osmFileName:     "build/parsemetadata_data.pbf",
		mappingFileName: "parsemetadata_yes_mapping.json",
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
	if ts.tableExists(t, dbschemaImport, "osm_parsemetadata_yes") != false {
		t.Fatalf("table osm_parsemetadata exists in schema %s", dbschemaImport)
	}
	ts.importOsm(t)
	if ts.tableExists(t, dbschemaImport, "osm_parsemetadata_yes") != true {
		t.Fatalf("table osm_parsemetadata does not exists in schema %s", dbschemaImport)
	}

	// =======================================================================
	t.Log("Deploy - step ")
	ts.deployOsm(t)
	if ts.tableExists(t, dbschemaImport, "osm_parsemetadata_yes") != false {
		t.Fatalf("table osm_parsemetadata_yes exists in schema %s", dbschemaImport)
	}
	if ts.tableExists(t, dbschemaProduction, "osm_parsemetadata_yes") != true {
		t.Fatalf("table osm_parsemetadata_yes does not exists in schema %s", dbschemaProduction)
	}

	// =======================================================================
	t.Log("Check - step  ")

	assertHstore(t, []checkElem{
		// Nodes
		{"osm_parsemetadata_yes", 31001, "tags", map[string]string{
			"created_by":  "JOSM",
			"_changeset_": "3000000",
			"_uid_":       "1",
			"_user_":      "u1",
			"_version_":   "1",
			"_timestamp_": "2000-11-11T00:11:11Z",
		}},
		{"osm_parsemetadata_yes", 31002, Missing, nil},
		{"osm_parsemetadata_yes", 31003, Missing, nil},
		{"osm_parsemetadata_yes", 31004, Missing, nil},
		{"osm_parsemetadata_yes", 31101, "tags", map[string]string{
			"amenity":            "cafe",
			"created_by":         "iDEditor",
			"testnote_version":   "11",
			"testnote_timestamp": "2011-11-11T01:11:11Z",
			"testnote_changeset": "3000001",
			"testnote_uid":       "301",
			"testnote_user":      "node301",
			"_version_":          "11",
			"_timestamp_":        "2011-11-11T01:11:11Z",
			"_changeset_":        "3000001",
			"_uid_":              "301",
			"_user_":             "node301",
		}},
		// Ways
		{"osm_parsemetadata_yes", -31002, "tags", map[string]string{
			"barrier":            "fence",
			"testnote_version":   "21",
			"testnote_timestamp": "2011-11-11T02:22:22Z",
			"testnote_changeset": "3000002",
			"testnote_uid":       "302",
			"testnote_user":      "way302",
			"_version_":          "21",
			"_timestamp_":        "2011-11-11T02:22:22Z",
			"_changeset_":        "3000002",
			"_uid_":              "302",
			"_user_":             "way302",
		}},
		{"osm_parsemetadata_yes", -31003, Missing, nil},
		{"osm_parsemetadata_yes", -31101, "tags", map[string]string{
			"highway":            "secondary",
			"testnote_version":   "21",
			"testnote_timestamp": "2011-11-11T02:22:22Z",
			"testnote_changeset": "3000002",
			"testnote_uid":       "302",
			"testnote_user":      "way302",
			"_version_":          "21",
			"_timestamp_":        "2011-11-11T02:22:22Z",
			"_changeset_":        "3000002",
			"_uid_":              "302",
			"_user_":             "way302",
		}},
		// Relations:
		{"osm_parsemetadata_yes", RelOffset - 31101, "tags", map[string]string{
			// "type":               "multipolygon",
			"building":           "yes",
			"testnote_version":   "31",
			"testnote_timestamp": "2011-11-11T03:33:33Z",
			"testnote_changeset": "3000003",
			"testnote_uid":       "303",
			"testnote_user":      "rel303",
			"_version_":          "31",
			"_timestamp_":        "2011-11-11T03:33:33Z",
			"_changeset_":        "3000003",
			"_uid_":              "303",
			"_user_":             "rel303",
		}},

		// overwrite test  - node1
		{"osm_parsemetadata_yes", 1, "tags", map[string]string{
			"amenity":     "cafe",
			"_version_":   "1",
			"_timestamp_": "2001-01-01T00:22:11Z",
			"_changeset_": "1",
			"_uid_":       "111",
			"_user_":      "z111",
		}},

		// overwrite test  - way1
		{"osm_parsemetadata_yes", -1, "tags", map[string]string{
			"highway":     "secondary",
			"_version_":   "1",
			"_timestamp_": "2001-01-01T00:22:22Z",
			"_changeset_": "1",
			"_uid_":       "111",
			"_user_":      "z111",
		}},
		// overwrite test  - rel1
		{"osm_parsemetadata_yes", RelOffset - 1, "tags", map[string]string{
			"amenity":     "cafe",
			"_version_":   "1",
			"_timestamp_": "2001-01-01T00:22:33Z",
			"_changeset_": "1",
			"_uid_":       "111",
			"_user_":      "z111",
		}},
	})

	// =======================================================================
	t.Log("Update with osc")
	ts.updateOsm(t, "./build/parsemetadata_data.osc.gz")

	// =======================================================================
	t.Log("Check osc ")
	assertHstore(t, []checkElem{
		// Nodes
		{"osm_parsemetadata_yes", 31001, Missing, nil},
		{"osm_parsemetadata_yes", 31002, "tags", map[string]string{
			"created_by":  "iDEditor",
			"_version_":   "12",
			"_timestamp_": "2012-12-22T00:22:11Z",
			"_changeset_": "4000001",
			"_uid_":       "311",
			"_user_":      "node311",
		}},
		{"osm_parsemetadata_yes", 31003, Missing, nil},
		{"osm_parsemetadata_yes", 31004, Missing, nil},
		//
		{"osm_parsemetadata_yes", 31101, "tags", map[string]string{
			"amenity":            "restaurant",
			"created_by":         "JOSM",
			"testnote_version":   "12",
			"testnote_timestamp": "2012-12-22T00:22:11Z",
			"testnote_changeset": "4000001",
			"testnote_uid":       "311",
			"testnote_user":      "node311",
			"_version_":          "12",
			"_timestamp_":        "2012-12-22T00:22:11Z",
			"_changeset_":        "4000001",
			"_uid_":              "311",
			"_user_":             "node311",
		}},
		// Ways
		{"osm_parsemetadata_yes", -31002, "tags", map[string]string{
			"barrier":            "fence",
			"access":             "private", // new
			"testnote_version":   "22",
			"testnote_timestamp": "2012-12-22T00:22:22Z",
			"testnote_changeset": "4000002",
			"testnote_uid":       "312",
			"testnote_user":      "way312",
			"_version_":          "22",
			"_timestamp_":        "2012-12-22T00:22:22Z",
			"_changeset_":        "4000002",
			"_uid_":              "312",
			"_user_":             "way312",
		}},
		{"osm_parsemetadata_yes", -31003, Missing, nil},
		{"osm_parsemetadata_yes", -31101, "tags", map[string]string{
			"highway":            "secondary",
			"landuse":            "park", // new
			"testnote_version":   "22",
			"testnote_timestamp": "2012-12-22T00:22:22Z",
			"testnote_changeset": "4000002",
			"testnote_uid":       "312",
			"testnote_user":      "way312",
			"_version_":          "22",
			"_timestamp_":        "2012-12-22T00:22:22Z",
			"_changeset_":        "4000002",
			"_uid_":              "312",
			"_user_":             "way312",
		}},
		// Relations:
		{"osm_parsemetadata_yes", RelOffset - 31101, "tags", map[string]string{
			// "type":            "multipolygon",       --  not added !
			"building":           "yes",
			"amenity":            "pub",                  // new
			"testnote_version":   "32",                   // "31"
			"testnote_timestamp": "2012-12-22T00:22:33Z", // "2011-11-11T03:33:33Z",
			"testnote_changeset": "4000003",              // "3000003",
			"testnote_uid":       "313",                  // "303",
			"testnote_user":      "rel313",               // "rel303",
			"_version_":          "32",                   // "31",
			"_timestamp_":        "2012-12-22T00:22:33Z", // "2011-11-11T03:33:33Z",
			"_changeset_":        "4000003",              // "3000003",
			"_uid_":              "313",                  // "303",
			"_user_":             "rel313",               // "rel303",
		}},

		// overwrite test    node1
		{"osm_parsemetadata_yes", 1, "tags", map[string]string{
			"amenity":     "cafe",
			"_version_":   "1",
			"_timestamp_": "2001-01-01T00:22:11Z",
			"_changeset_": "1",
			"_uid_":       "111",
			"_user_":      "z111",
		}},
		// overwrite test    way1
		{"osm_parsemetadata_yes", -1, "tags", map[string]string{
			"highway":     "secondary",
			"_version_":   "1",
			"_timestamp_": "2001-01-01T00:22:22Z",
			"_changeset_": "1",
			"_uid_":       "111",
			"_user_":      "z111",
		}},
		// overwrite test   rel1
		{"osm_parsemetadata_yes", RelOffset - 1, "tags", map[string]string{
			"amenity":     "cafe",
			"_version_":   "1",
			"_timestamp_": "2001-01-01T00:22:33Z",
			"_changeset_": "1",
			"_uid_":       "111",
			"_user_":      "z111",
		}},
	})

	t.Log("-- end -- ")
}
