package test

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"strings"
	"testing"

	"github.com/omniscale/imposm3/element"

	"github.com/omniscale/imposm3/cache"

	"github.com/lib/pq/hstore"

	"github.com/omniscale/imposm3/geom/geos"
	"github.com/omniscale/imposm3/update"

	"github.com/omniscale/imposm3/config"
	"github.com/omniscale/imposm3/import_"
)

const (
	dbschemaImport     = "imposm3testimport"
	dbschemaProduction = "imposm3testproduction"
	dbschemaBackup     = "imposm3testbackup"
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
	config importConfig
	db     *sql.DB
	g      *geos.Geos
}

const Missing = ""

func (s *importTestSuite) importOsm(t *testing.T) {
	importArgs := []string{
		"-connection", s.config.connection,
		"-read", s.config.osmFileName,
		"-write",
		"-cachedir", s.config.cacheDir,
		"-diff",
		"-overwritecache",
		"-dbschema-import", dbschemaImport,
		// "-optimize",
		"-mapping", s.config.mappingFileName,
		"-quiet",
		"-revertdeploy=false",
		"-deployproduction=false",
		"-removebackup=false",
	}

	config.ParseImport(importArgs)
	import_.Import()
}

func (s *importTestSuite) deployOsm(t *testing.T) {
	importArgs := []string{
		"-read=", // overwrite previous options
		"-write=false",
		"-optimize=false",
		"-revertdeploy=false",
		"-deployproduction",
		"-removebackup=false",
		"-connection", s.config.connection,
		"-dbschema-import", dbschemaImport,
		"-dbschema-production", dbschemaProduction,
		"-dbschema-backup", dbschemaBackup,
		"-deployproduction",
		"-mapping", s.config.mappingFileName,
		"-quiet",
	}

	config.ParseImport(importArgs)
	import_.Import()
}

func (s *importTestSuite) revertDeployOsm(t *testing.T) {
	importArgs := []string{
		"-read=", // overwrite previous options
		"-write=false",
		"-optimize=false",
		"-revertdeploy",
		"-deployproduction=false",
		"-removebackup=false",
		"-connection", s.config.connection,
		"-dbschema-import", dbschemaImport,
		"-dbschema-production", dbschemaProduction,
		"-dbschema-backup", dbschemaBackup,
		"-revertdeploy",
		"-deployproduction=false",
		"-removebackup=false",
		"-mapping", s.config.mappingFileName,
		"-quiet",
	}

	config.ParseImport(importArgs)
	import_.Import()
}

func (s *importTestSuite) cache(t *testing.T) *cache.OSMCache {
	c := cache.NewOSMCache(s.config.cacheDir)
	if err := c.Open(); err != nil {
		t.Fatal(err)
	}
	return c
}

func (s *importTestSuite) diffCache(t *testing.T) *cache.DiffCache {
	c := cache.NewDiffCache(s.config.cacheDir)
	if err := c.Open(); err != nil {
		t.Fatal(err)
	}
	return c
}

func (s *importTestSuite) removeBackupOsm(t *testing.T) {
	importArgs := []string{
		"-read=", // overwrite previous options
		"-write=false",
		"-optimize=false",
		"-revertdeploy=false",
		"-deployproduction=false",
		"-removebackup",
		"-connection", s.config.connection,
		"-dbschema-import", dbschemaImport,
		"-dbschema-production", dbschemaProduction,
		"-dbschema-backup", dbschemaBackup,
		"-mapping", s.config.mappingFileName,
		"-quiet",
	}

	config.ParseImport(importArgs)
	import_.Import()
}

func (s *importTestSuite) updateOsm(t *testing.T, diffFile string) {
	args := []string{
		"-connection", s.config.connection,
		"-cachedir", s.config.cacheDir,
		"-limitto", "clipping.geojson",
		"-dbschema-production", dbschemaProduction,
		"-mapping", s.config.mappingFileName,
	}
	if s.config.expireTileDir != "" {
		args = append(args, "-expiretiles-dir", s.config.expireTileDir)
	}
	args = append(args, diffFile)
	config.ParseDiffImport(args)
	update.Diff()
}

func (s *importTestSuite) dropSchemas() {
	var err error
	_, err = s.db.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, dbschemaImport))
	if err != nil {
		log.Fatal(err)
	}
	_, err = s.db.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, dbschemaProduction))
	if err != nil {
		log.Fatal(err)
	}
	_, err = s.db.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, dbschemaBackup))
	if err != nil {
		log.Fatal(err)
	}
}

func (s *importTestSuite) tableExists(t *testing.T, schema, table string) bool {
	row := s.db.QueryRow(fmt.Sprintf(`SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='%s' AND table_schema='%s')`, table, schema))
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

func (s *importTestSuite) query(t *testing.T, table string, id int64, keys []string) record {
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
	stmt := fmt.Sprintf(`SELECT osm_id, name, type, ST_AsText(geometry), %s FROM "%s"."%s" WHERE osm_id=$1`, columns, dbschemaProduction, table)
	row := s.db.QueryRow(stmt, id)
	r := record{}
	h := hstore.Hstore{}
	if err := row.Scan(&r.id, &r.name, &r.osmType, &r.wkt, &h); err != nil {
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

func (s *importTestSuite) queryTags(t *testing.T, table string, id int64) record {
	stmt := fmt.Sprintf(`SELECT osm_id, tags FROM "%s"."%s" WHERE osm_id=$1`, dbschemaProduction, table)
	row := s.db.QueryRow(stmt, id)
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

func (s *importTestSuite) queryRows(t *testing.T, table string, id int64) []record {
	rows, err := s.db.Query(fmt.Sprintf(`SELECT osm_id, name, type, ST_AsText(geometry) FROM "%s"."%s" WHERE osm_id=$1 ORDER BY type, name, ST_GeometryType(geometry)`, dbschemaProduction, table), id)
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

func (s *importTestSuite) queryRowsTags(t *testing.T, table string, id int64) []record {
	rows, err := s.db.Query(fmt.Sprintf(`SELECT osm_id, ST_AsText(geometry), tags FROM "%s"."%s" WHERE osm_id=$1 ORDER BY ST_GeometryType(geometry)`, dbschemaProduction, table), id)
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

func (s *importTestSuite) queryGeom(t *testing.T, table string, id int64) *geos.Geom {
	stmt := fmt.Sprintf(`SELECT osm_id, ST_AsText(geometry) FROM "%s"."%s" WHERE osm_id=$1`, dbschemaProduction, table)
	row := s.db.QueryRow(stmt, id)
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

func (s *importTestSuite) queryDynamic(t *testing.T, table, where string) []map[string]string {
	stmt := fmt.Sprintf(`SELECT hstore(r) FROM (SELECT ST_AsText(geometry) AS wkt, * FROM "%s"."%s" WHERE %s) AS r`, dbschemaProduction, table, where)
	rows, err := s.db.Query(stmt)
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

func assertRecords(t *testing.T, elems []checkElem) {
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

func assertHstore(t *testing.T, elems []checkElem) {
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

func assertGeomValid(t *testing.T, e checkElem) {
	geom := ts.queryGeom(t, e.table, e.id)
	if !ts.g.IsValid(geom) {
		t.Fatalf("geometry of %d is invalid", e.id)
	}
}

func assertGeomArea(t *testing.T, e checkElem, expect float64) {
	geom := ts.queryGeom(t, e.table, e.id)
	if !ts.g.IsValid(geom) {
		t.Fatalf("geometry of %d is invalid", e.id)
	}
	actual := geom.Area()
	if math.Abs(expect-actual) > 1 {
		t.Errorf("unexpected size of %d %f!=%f", e.id, actual, expect)
	}
}

func assertGeomLength(t *testing.T, e checkElem, expect float64) {
	geom := ts.queryGeom(t, e.table, e.id)
	if !ts.g.IsValid(geom) {
		t.Fatalf("geometry of %d is invalid", e.id)
	}
	actual := geom.Length()
	if math.Abs(expect-actual) > 1 {
		t.Errorf("unexpected size of %d %f!=%f", e.id, actual, expect)
	}
}

func assertGeomType(t *testing.T, e checkElem, expect string) {
	actual := ts.g.Type(ts.queryGeom(t, e.table, e.id))
	if actual != expect {
		t.Errorf("expected %s geometry for %d, got %s", expect, e.id, actual)
	}
}

func assertCachedWay(t *testing.T, c *cache.OSMCache, id int64) *element.Way {
	way, err := c.Ways.GetWay(id)
	if err == cache.NotFound {
		t.Errorf("missing way %d", id)
	} else if err != nil {
		t.Fatal(err)
	}
	if way.Id != id {
		t.Errorf("cached way contains invalid id, %d != %d", way.Id, id)
	}
	return way
}

func assertCachedNode(t *testing.T, c *cache.OSMCache, id int64) *element.Node {
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
	if node.Id != id {
		t.Errorf("cached node contains invalid id, %d != %d", node.Id, id)
	}
	return node
}
