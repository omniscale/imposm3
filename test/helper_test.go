package test

import (
	"database/sql"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/lib/pq/hstore"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/config"
	"github.com/omniscale/imposm3/geom/geos"
	"github.com/omniscale/imposm3/import_"
	"github.com/omniscale/imposm3/log"
	"github.com/omniscale/imposm3/update"
)

type importConfig struct {
	connection      string
	osmFileName     string
	mappingFileName string
	cacheDir        string
	verbose         bool
	expireTileDir   string
}

type importTestSuite struct {
	dir    string
	name   string
	config importConfig
	db     *sql.DB
	g      *geos.Geos
}

const Missing = ""

func (ts *importTestSuite) dbschemaImport() string { return "imposm_test_" + ts.name + "_import" }
func (ts *importTestSuite) dbschemaProduction() string {
	return "imposm_test_" + ts.name + "_production"
}
func (ts *importTestSuite) dbschemaBackup() string { return "imposm_test_" + ts.name + "_backup" }

func (ts *importTestSuite) importOsm(t *testing.T) {
	importArgs := []string{
		"-connection", ts.config.connection,
		"-read", ts.config.osmFileName,
		"-write",
		"-cachedir", ts.config.cacheDir,
		"-diff",
		"-overwritecache",
		"-dbschema-import", ts.dbschemaImport(),
		"-optimize",
		"-mapping", ts.config.mappingFileName,
		"-quiet",
		"-revertdeploy=false",
		"-deployproduction=false",
		"-removebackup=false",
	}

	opts := config.ParseImport(importArgs)
	import_.Import(opts)
}

func (ts *importTestSuite) deployOsm(t *testing.T) {
	importArgs := []string{
		"-read=", // overwrite previous options
		"-write=false",
		"-optimize=false",
		"-revertdeploy=false",
		"-deployproduction",
		"-removebackup=false",
		"-connection", ts.config.connection,
		"-dbschema-import", ts.dbschemaImport(),
		"-dbschema-production", ts.dbschemaProduction(),
		"-dbschema-backup", ts.dbschemaBackup(),
		"-deployproduction",
		"-mapping", ts.config.mappingFileName,
		"-quiet",
	}

	opts := config.ParseImport(importArgs)
	import_.Import(opts)
}

func (ts *importTestSuite) revertDeployOsm(t *testing.T) {
	importArgs := []string{
		"-read=", // overwrite previous options
		"-write=false",
		"-optimize=false",
		"-revertdeploy",
		"-deployproduction=false",
		"-removebackup=false",
		"-connection", ts.config.connection,
		"-dbschema-import", ts.dbschemaImport(),
		"-dbschema-production", ts.dbschemaProduction(),
		"-dbschema-backup", ts.dbschemaBackup(),
		"-revertdeploy",
		"-deployproduction=false",
		"-removebackup=false",
		"-mapping", ts.config.mappingFileName,
		"-quiet",
	}

	opts := config.ParseImport(importArgs)
	import_.Import(opts)
}

func (ts *importTestSuite) cache(t *testing.T) *cache.OSMCache {
	c := cache.NewOSMCache(ts.config.cacheDir)
	if err := c.Open(); err != nil {
		t.Fatal(err)
	}
	return c
}

func (ts *importTestSuite) diffCache(t *testing.T) *cache.DiffCache {
	c := cache.NewDiffCache(ts.config.cacheDir)
	if err := c.Open(); err != nil {
		t.Fatal(err)
	}
	return c
}

func (ts *importTestSuite) removeBackupOsm(t *testing.T) {
	importArgs := []string{
		"-read=", // overwrite previous options
		"-write=false",
		"-optimize=false",
		"-revertdeploy=false",
		"-deployproduction=false",
		"-removebackup",
		"-connection", ts.config.connection,
		"-dbschema-import", ts.dbschemaImport(),
		"-dbschema-production", ts.dbschemaProduction(),
		"-dbschema-backup", ts.dbschemaBackup(),
		"-mapping", ts.config.mappingFileName,
		"-quiet",
	}

	opts := config.ParseImport(importArgs)
	import_.Import(opts)
}

func (ts *importTestSuite) updateOsm(t *testing.T, diffFile string) {
	args := []string{
		"-connection", ts.config.connection,
		"-cachedir", ts.config.cacheDir,
		"-limitto", "clipping.geojson",
		"-dbschema-production", ts.dbschemaProduction(),
		"-mapping", ts.config.mappingFileName,
	}
	if ts.config.expireTileDir != "" {
		args = append(args, "-expiretiles-dir", ts.config.expireTileDir)
	}
	args = append(args, diffFile)
	opts, files := config.ParseDiffImport(args)
	update.Diff(opts, files)
}

func (ts *importTestSuite) dropSchemas() {
	var err error
	_, err = ts.db.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, ts.dbschemaImport()))
	if err != nil {
		log.Fatal(err)
	}
	_, err = ts.db.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, ts.dbschemaProduction()))
	if err != nil {
		log.Fatal(err)
	}
	_, err = ts.db.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, ts.dbschemaBackup()))
	if err != nil {
		log.Fatal(err)
	}
}

func (ts *importTestSuite) tableExists(t *testing.T, schema, table string) bool {
	row := ts.db.QueryRow(
		`SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=$1 AND table_schema=$2)`,
		table, schema,
	)
	var exists bool
	if err := row.Scan(&exists); err != nil {
		t.Error(err)
		return false
	}
	return exists
}

func (ts *importTestSuite) indexExists(t *testing.T, schema, table, index string) bool {
	row := ts.db.QueryRow(
		`SELECT EXISTS(SELECT * FROM pg_indexes WHERE tablename=$1 AND schemaname=$2 AND indexname like $3)`,
		table, schema, index,
	)
	var exists bool
	if err := row.Scan(&exists); err != nil {
		t.Error(err)
		return false
	}
	return exists
}

type record struct {
	id      int
	name    string
	osmType string
	wkt     string
	missing bool
	tags    map[string]string
}

func (ts *importTestSuite) query(t *testing.T, table string, id int64, keys []string) record {
	kv := make([]string, len(keys))
	for i, k := range keys {
		kv[i] = "'" + k + "', " + k + "::varchar"
	}
	columns := strings.Join(kv, ", ")
	if columns == "" {
		columns = "''::hstore"
	} else {
		columns = "hstore(ARRAY[" + columns + "])"
	}
	stmt := fmt.Sprintf(`SELECT osm_id, name, type, ST_AsText(geometry), %s FROM "%s"."%s" WHERE osm_id=$1`, columns, ts.dbschemaProduction(), table)
	rows, err := ts.db.Query(stmt, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	r := record{}
	h := hstore.Hstore{}

	if !rows.Next() {
		r.missing = true
		return r
	}
	if err := rows.Scan(&r.id, &r.name, &r.osmType, &r.wkt, &h); err != nil {
		t.Fatal(err)
	}
	if len(h.Map) > 0 {
		r.tags = make(map[string]string)
	}
	for k, v := range h.Map {
		if v.Valid {
			r.tags[k] = v.String
		}
	}

	if rows.Next() {
		t.Errorf("duplicate row for %d in %q", id, table)
	}
	return r
}

func (ts *importTestSuite) queryTags(t *testing.T, table string, id int64) record {
	stmt := fmt.Sprintf(`SELECT osm_id, tags FROM "%s"."%s" WHERE osm_id=$1`, ts.dbschemaProduction(), table)
	row := ts.db.QueryRow(stmt, id)
	r := record{}
	h := hstore.Hstore{}
	if err := row.Scan(&r.id, &h); err != nil {
		if err == sql.ErrNoRows {
			r.missing = true
		} else {
			t.Fatal(err)
		}
	}
	if len(h.Map) > 0 {
		r.tags = make(map[string]string)
	}
	for k, v := range h.Map {
		if v.Valid {
			r.tags[k] = v.String
		}
	}
	return r
}

func (ts *importTestSuite) queryRows(t *testing.T, table string, id int64) []record {
	rows, err := ts.db.Query(fmt.Sprintf(`SELECT osm_id, name, type, ST_AsText(geometry) FROM "%s"."%s" WHERE osm_id=$1 ORDER BY type, name, ST_GeometryType(geometry)`, ts.dbschemaProduction(), table), id)
	if err != nil {
		t.Fatal(err)
	}
	rs := []record{}
	for rows.Next() {
		var r record
		if err := rows.Scan(&r.id, &r.name, &r.osmType, &r.wkt); err != nil {
			t.Fatal(err)
		}
		rs = append(rs, r)
	}
	return rs
}

func (ts *importTestSuite) queryRowsTags(t *testing.T, table string, id int64) []record {
	rows, err := ts.db.Query(fmt.Sprintf(`SELECT osm_id, ST_AsText(geometry), tags FROM "%s"."%s" WHERE osm_id=$1 ORDER BY ST_GeometryType(geometry)`, ts.dbschemaProduction(), table), id)
	if err != nil {
		t.Fatal(err)
	}
	rs := []record{}
	for rows.Next() {
		var r record
		h := hstore.Hstore{}
		if err := rows.Scan(&r.id, &r.wkt, &h); err != nil {
			t.Fatal(err)
		}
		if len(h.Map) > 0 {
			r.tags = make(map[string]string)
		}
		for k, v := range h.Map {
			if v.Valid {
				r.tags[k] = v.String
			}
		}
		rs = append(rs, r)
	}
	return rs
}

func (ts *importTestSuite) queryGeom(t *testing.T, table string, id int64) *geos.Geom {
	stmt := fmt.Sprintf(`SELECT osm_id, ST_AsText(geometry) FROM "%s"."%s" WHERE osm_id=$1`, ts.dbschemaProduction(), table)
	row := ts.db.QueryRow(stmt, id)
	r := record{}
	if err := row.Scan(&r.id, &r.wkt); err != nil {
		if err == sql.ErrNoRows {
			r.missing = true
		} else {
			t.Fatal(err)
		}
	}
	g := geos.NewGeos()
	defer g.Finish()
	geom := g.FromWkt(r.wkt)
	if geom == nil {
		t.Fatalf("unable to read WKT for %d", id)
	}
	return geom
}

func (ts *importTestSuite) queryDynamic(t *testing.T, table, where string) []map[string]string {
	stmt := fmt.Sprintf(`SELECT hstore(r) FROM (SELECT ST_AsText(geometry) AS wkt, * FROM "%s"."%s" WHERE %s) AS r`, ts.dbschemaProduction(), table, where)
	rows, err := ts.db.Query(stmt)
	if err != nil {
		t.Fatal(err)
	}
	results := []map[string]string{}
	for rows.Next() {
		h := hstore.Hstore{}
		if err := rows.Scan(&h); err != nil {
			t.Fatal(err)
		}
		r := make(map[string]string)
		for k, v := range h.Map {
			if v.Valid {
				r[k] = v.String
			}
		}
		results = append(results, r)
	}
	return results
}

type checkElem struct {
	table   string
	id      int64
	osmType string
	tags    map[string]string
}

func (ts *importTestSuite) assertRecords(t *testing.T, elems []checkElem) {
	for _, e := range elems {
		keys := make([]string, 0, len(e.tags))
		for k, _ := range e.tags {
			keys = append(keys, k)
		}
		r := ts.query(t, e.table, e.id, keys)
		if e.osmType == "" {
			if r.missing {
				continue
			}
			t.Errorf("got unexpected record %d", r.id)
		}
		if r.osmType != e.osmType {
			t.Errorf("got unexpected type %s != %s for %d", r.osmType, e.osmType, e.id)
		}
		for k, v := range e.tags {
			if r.tags[k] != v {
				t.Errorf("%s does not match for %d %s != %s", k, e.id, r.tags[k], v)
			}
		}
	}
}

func (ts *importTestSuite) assertHstore(t *testing.T, elems []checkElem) {
	for _, e := range elems {
		r := ts.queryTags(t, e.table, e.id)
		if e.osmType == "" {
			if r.missing {
				continue
			}
			t.Errorf("got unexpected record %d", r.id)
		}
		if len(e.tags) != len(r.tags) {
			t.Errorf("tags for %d differ %v != %v", e.id, r.tags, e.tags)
		}
		for k, v := range e.tags {
			if r.tags[k] != v {
				t.Errorf("%s does not match for %d %s != %s", k, e.id, r.tags[k], v)
			}
		}
	}
}

func (ts *importTestSuite) assertGeomValid(t *testing.T, e checkElem) {
	geom := ts.queryGeom(t, e.table, e.id)
	if !ts.g.IsValid(geom) {
		t.Fatalf("geometry of %d is invalid", e.id)
	}
}

func (ts *importTestSuite) assertGeomArea(t *testing.T, e checkElem, expect float64) {
	geom := ts.queryGeom(t, e.table, e.id)
	if !ts.g.IsValid(geom) {
		t.Fatalf("geometry of %d is invalid", e.id)
	}
	actual := geom.Area()
	if math.Abs(expect-actual) > 1 {
		t.Errorf("unexpected size of %d %f!=%f", e.id, actual, expect)
	}
}

func (ts *importTestSuite) assertGeomLength(t *testing.T, e checkElem, expect float64) {
	geom := ts.queryGeom(t, e.table, e.id)
	if !ts.g.IsValid(geom) {
		t.Fatalf("geometry of %d is invalid", e.id)
	}
	actual := geom.Length()
	if math.Abs(expect-actual) > 1 {
		t.Errorf("unexpected size of %d %f!=%f", e.id, actual, expect)
	}
}

func (ts *importTestSuite) assertGeomType(t *testing.T, e checkElem, expect string) {
	actual := ts.g.Type(ts.queryGeom(t, e.table, e.id))
	if actual != expect {
		t.Errorf("expected %s geometry for %d, got %s", expect, e.id, actual)
	}
}

func (ts *importTestSuite) assertCachedWay(t *testing.T, c *cache.OSMCache, id int64) *osm.Way {
	way, err := c.Ways.GetWay(id)
	if err == cache.NotFound {
		t.Errorf("missing way %d", id)
	} else if err != nil {
		t.Fatal(err)
	}
	if way.ID != id {
		t.Errorf("cached way contains invalid id, %d != %d", way.ID, id)
	}
	return way
}

func (ts *importTestSuite) assertCachedNode(t *testing.T, c *cache.OSMCache, id int64) *osm.Node {
	node, err := c.Nodes.GetNode(id)
	if err == cache.NotFound {
		node, err = c.Coords.GetCoord(id)
		if err == cache.NotFound {
			t.Errorf("missing node %d", id)
			return nil
		}
	} else if err != nil {
		t.Fatal(err)
	}
	if node.ID != id {
		t.Errorf("cached node contains invalid id, %d != %d", node.ID, id)
	}
	return node
}
