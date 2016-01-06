package test

import (
	"database/sql"
	"strconv"

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

func TestRouteRelation_RelationData(t *testing.T) {
	// check tags of relation
	r := ts.queryTags(t, "osm_routes", -100901)
	if r.tags["name"] != "Bus 301: A => B" {
		t.Error(r)
	}
}

func TestRouteRelation_MemberGeomUpdated1(t *testing.T) {
	rows := ts.queryDynamic(t, "osm_route_members", "osm_id = -100902 AND member = 100502")
	if len(rows) != 1 {
		t.Fatal(rows)
	}
	g := ts.g.FromWkt(rows[0]["wkt"])
	if g.Length() != 111.32448543701321 {
		t.Fatal(g.Length())
	}

	rows = ts.queryDynamic(t, "osm_route_members", "osm_id = -100902 AND member = 100503")
	if len(rows) != 1 {
		t.Fatal(rows)
	}
	if rows[0]["name"] != "" {
		t.Error(rows[0])
	}
}

// #######################################################################

func TestRouteRelation_Update(t *testing.T) {
	ts.updateOsm(t, "./build/route_relation.osc.gz")
}

// #######################################################################

func TestRouteRelation_MemberGeomUpdated2(t *testing.T) {
	rows := ts.queryDynamic(t, "osm_route_members", "osm_id = -100902 AND member = 100502")
	if len(rows) != 1 {
		t.Fatal(rows)
	}
	g := ts.g.FromWkt(rows[0]["wkt"])
	if g.Length() != 184.97560221624542 {
		t.Fatal(g.Length())
	}

	rows = ts.queryDynamic(t, "osm_route_members", "osm_id = -100902 AND member = 100503")
	if len(rows) != 1 {
		t.Fatal(rows)
	}
	if rows[0]["name"] != "new name" {
		t.Error(rows[0])
	}
}

func TestRouteRelation_MemberNotUpdated(t *testing.T) {
	// check that member is not updated if no node/way changed
	rows := ts.queryDynamic(t, "osm_route_members", "osm_id = -100903 AND member = 100501")
	if len(rows) != 1 {
		t.Fatal(rows)
	}
	if id, err := strconv.ParseInt(rows[0]["id"], 10, 32); err != nil || id > 27 {
		t.Error("member was re-inserted", rows)
	}

}
