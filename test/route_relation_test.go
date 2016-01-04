package test

import (
	"database/sql"

	"testing"

	"github.com/omniscale/imposm3/geom/geos"
)

func TestRouteRelation_Prepare(t *testing.T) {
	ts.dir = "/tmp/imposm3test"
	ts.config = importConfig{
		connection:      "postgis://",
		cacheDir:        ts.dir,
		osmFileName:     "build/route_relation.pbf",
		mappingFileName: "route_relation_mapping.yml",
	}
	ts.g = geos.NewGeos()

	var err error
	ts.db, err = sql.Open("postgres", "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	ts.dropSchemas()
}

func TestRouteRelation_Import(t *testing.T) {
	if ts.tableExists(t, dbschemaImport, "osm_routes") != false {
		t.Fatalf("table osm_routes exists in schema %s", dbschemaImport)
	}
	ts.importOsm(t)
	if ts.tableExists(t, dbschemaImport, "osm_routes") != true {
		t.Fatalf("table osm_routes does not exists in schema %s", dbschemaImport)
	}
}

func TestRouteRelation_Deploy(t *testing.T) {
	ts.deployOsm(t)
	if ts.tableExists(t, dbschemaImport, "osm_routes") != false {
		t.Fatalf("table osm_routes exists in schema %s", dbschemaImport)
	}
	if ts.tableExists(t, dbschemaProduction, "osm_routes") != true {
		t.Fatalf("table osm_routes does not exists in schema %s", dbschemaProduction)
	}
}

// #######################################################################

func TestRouteRelation_Update(t *testing.T) {
	ts.updateOsm(t, "./build/route_relation.osc.gz")
}
