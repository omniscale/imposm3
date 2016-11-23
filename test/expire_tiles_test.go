package test

import (
	"bufio"
	"database/sql"
	"github.com/omniscale/imposm3/expire"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"testing"

	"github.com/omniscale/imposm3/geom/geos"
)

func TestExpireTiles_Prepare(t *testing.T) {
	var err error

	ts.dir, err = ioutil.TempDir("", "imposm3test")
	if err != nil {
		t.Fatal(err)
	}
	ts.config = importConfig{
		connection:      "postgis://",
		cacheDir:        ts.dir,
		osmFileName:     "build/expire_tiles.pbf",
		mappingFileName: "expire_tiles_mapping.yml",
		expireTileDir:   filepath.Join(ts.dir, "expiretiles"),
	}
	ts.g = geos.NewGeos()

	ts.db, err = sql.Open("postgres", "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	ts.dropSchemas()
}

func TestExpireTiles_Import(t *testing.T) {
	if ts.tableExists(t, dbschemaImport, "osm_roads") != false {
		t.Fatalf("table osm_roads exists in schema %s", dbschemaImport)
	}
	ts.importOsm(t)
	ts.deployOsm(t)
	if ts.tableExists(t, dbschemaProduction, "osm_roads") != true {
		t.Fatalf("table osm_roads does not exists in schema %s", dbschemaProduction)
	}
}

func TestExpireTiles_Elements(t *testing.T) {
	assertRecords(t, []checkElem{
		{"osm_roads", 20151, "motorway", nil},
		{"osm_roads", 20251, "motorway", nil},
		{"osm_roads", 20351, "motorway", nil},

		{"osm_buildings", -30191, "yes", nil},
		{"osm_buildings", -30291, "yes", nil},
		{"osm_buildings", -30391, "yes", nil},
		{"osm_buildings", -30491, "yes", nil},
	})
}

func TestExpireTiles_Update(t *testing.T) {
	ts.updateOsm(t, "build/expire_tiles.osc.gz")
}

func TestExpireTiles_CheckExpireFile(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(ts.config.expireTileDir, "*", "*.tiles"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("expected one expire tile file, got: %v", files)
	}
	tiles, err := parseTileList(files[0])
	if err != nil {
		t.Error(err)
	}

	for _, test := range []struct {
		reason string
		long   float64
		lat    float64
		expire bool
	}{
		{"create node", 3, 1, true},
		{"modify node (old)", 1, 1, true},
		{"modify node (new)", 1, -1, true},
		{"modify node to unmapped (old)", 4, 1, true},
		{"modify node to unmapped (new)", 4, -1, false},
		{"delete node", 2, 1, true},

		{"delete way", 2.0001, 2, true},
		{"modify way", 1.0001, 2, true},
		{"modify way from node (old)", 3.0001, 2, true},
		{"modify way from node (new)", 3.0001, -2, true},
		{"create way", 4.0001, 2, true},

		{"create long way (start)", 5.00, 2, true},
		{"create long way (mid)", 5.025, 2, false}, // TODO not implemented
		{"create long way (end)", 5.05, 2, true},

		{"modify relation", 1.0001, 3, true},
		{"delete relation", 2.0001, 3, true},
		{"modify relation from way", 3.0001, 3, true},
		{"modify relation from nodes (old)", 4.0001, 3, true},
		{"modify relation from nodes (new)", 4.0001, -3, true},
	} {
		for _, coord := range expire.TileCoords(test.long, test.lat, 14) {
			x, y := coord.X, coord.Y
			if test.expire {
				if _, ok := tiles[tile{x: int(x), y: int(y), z: 14}]; !ok {
					t.Errorf("missing expire tile for %s 14/%d/%d for %f %f", test.reason, x, y, test.long, test.lat)
				} else {
					delete(tiles, tile{x: int(x), y: int(y), z: 14})
				}
			} else {
				if _, ok := tiles[tile{x: int(x), y: int(y), z: 14}]; ok {
					t.Errorf("found expire tile for %s 14/%d/%d for %f %f", test.reason, x, y, test.long, test.lat)
				}
			}
		}
	}

	for tile, _ := range tiles {
		t.Errorf("unexpected tile expired: %v", tile)
	}
}

func TestExpireTiles_Cleanup(t *testing.T) {
	ts.dropSchemas()
	if err := os.RemoveAll(ts.dir); err != nil {
		t.Error(err)
	}
}

type tile struct {
	x, y, z int
}

func parseTileList(filename string) (map[tile]struct{}, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	tiles := make(map[tile]struct{})
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), "/")
		z, _ := strconv.ParseInt(parts[0], 10, 32)
		x, _ := strconv.ParseInt(parts[1], 10, 32)
		y, _ := strconv.ParseInt(parts[2], 10, 32)
		tiles[tile{x: int(x), y: int(y), z: int(z)}] = struct{}{}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return tiles, nil
}
