package test

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/lib/pq/hstore"

	"github.com/omniscale/imposm3/geom/geos"

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
}

type importTestSuite struct {
	dir    string
	config importConfig
	db     *sql.DB
	g      *geos.Geos
}

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
		"-deployproduction=false",
	}

	config.ParseImport(importArgs)
	import_.Import()
}

func (s *importTestSuite) deployOsm(t *testing.T) {
	importArgs := []string{
		"-read=", // overwrite previous options
		"-write=false",
		"-optimize=false",
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

func (s *importTestSuite) queryExists(t *testing.T, table string, id int64) bool {
	row := s.db.QueryRow(fmt.Sprintf(`SELECT EXISTS(SELECT * FROM "%s"."%s" WHERE osm_id=$1)`, dbschemaProduction, table), id)
	var exists bool
	if err := row.Scan(&exists); err != nil {
		t.Error(err)
		return false
	}
	return exists
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

func (s *importTestSuite) queryRows(t *testing.T, table string, id int64) []record {
	rows, err := s.db.Query(fmt.Sprintf(`SELECT osm_id, name, type, ST_AsText(geometry) FROM "%s"."%s" WHERE osm_id=$1 ORDER BY type, name`, dbschemaProduction, table), id)
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

func (s *importTestSuite) queryGeom(t *testing.T, table string, id int64) *geos.Geom {
	r := s.query(t, table, id, nil)
	g := geos.NewGeos()
	defer g.Finish()
	geom := g.FromWkt(r.wkt)
	if geom == nil {
		t.Fatalf("unable to read WKT for %s", id)
	}
	return geom
}
