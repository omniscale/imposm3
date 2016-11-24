package test

import (
	"bufio"
	"database/sql"
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
		tiles  []tile
		expire bool
	}{
		{"create node", []tile{{8328, 8146, 14}}, true},
		{"modify node (old)", []tile{{8237, 8146, 14}}, true},
		{"modify node (new)", []tile{{8237, 8237, 14}}, true},
		{"modify node to unmapped (old)", []tile{{8373, 8146, 14}, {8374, 8146, 14}}, true},
		{"modify node to unmapped (new)", []tile{{8373, 8146, 14}, {8374, 8146, 14}}, false},
		{"delete node", []tile{{8282, 8146, 14}, {8283, 8146, 14}}, true},

		{"delete way", []tile{{8283, 8100, 14}}, true},
		{"modify way", []tile{{8237, 8100, 14}}, true},
		{"modify way from node (old)", []tile{{8328, 8100, 14}}, true},
		{"modify way from node (new)", []tile{{8328, 8283, 14}}, true},
		{"create way", []tile{{8374, 8100, 14}}, true},
		{"create long way", []tile{{8419, 8100, 14}, {8420, 8100, 14}, {8421, 8100, 14}}, true},

		{"modify relation", []tile{{8237, 8055, 14}}, true},
		{"delete relation", []tile{{8283, 8055, 14}}, true},
		{"modify relation from way", []tile{{8328, 8055, 14}}, true},
		{"modify relation from nodes (old)", []tile{{8374, 8055, 14}}, true},
		{"modify relation from nodes (new)", []tile{{8374, 8328, 14}}, true},
		{"create polygon (box)", []tile{
			{8237, 8007, 14},
			{8237, 8008, 14},
			{8237, 8009, 14},
			{8238, 8007, 14},
			{8238, 8008, 14},
			{8238, 8009, 14},
			{8239, 8007, 14},
			{8239, 8008, 14},
			{8239, 8009, 14},
		}, true},

		{"create polygon (outline)", []tile{
			{8310, 8005, 14}, {8302, 7991, 14}, {8283, 7993, 14},
			{8300, 8009, 14}, {8283, 8003, 14}, {8308, 8009, 14},
			{8310, 7995, 14}, {8285, 8009, 14}, {8288, 8009, 14},
			{8301, 8009, 14}, {8310, 8002, 14}, {8302, 8009, 14},
			{8310, 8003, 14}, {8286, 8009, 14}, {8300, 7991, 14},
			{8283, 7994, 14}, {8296, 8009, 14}, {8298, 8009, 14},
			{8310, 8009, 14}, {8283, 7999, 14}, {8283, 7992, 14},
			{8290, 7991, 14}, {8305, 8009, 14}, {8309, 7991, 14},
			{8306, 7991, 14}, {8291, 7991, 14}, {8283, 7996, 14},
			{8310, 7996, 14}, {8293, 7991, 14}, {8310, 8007, 14},
			{8310, 8001, 14}, {8307, 8009, 14}, {8299, 8009, 14},
			{8310, 7998, 14}, {8310, 7999, 14}, {8301, 7991, 14},
			{8283, 7998, 14}, {8283, 8006, 14}, {8289, 8009, 14},
			{8310, 8008, 14}, {8285, 7991, 14}, {8283, 8002, 14},
			{8289, 7991, 14}, {8286, 7991, 14}, {8288, 7991, 14},
			{8283, 8008, 14}, {8283, 8005, 14}, {8310, 7992, 14},
			{8310, 8004, 14}, {8310, 7991, 14}, {8296, 7991, 14},
			{8292, 7991, 14}, {8283, 8009, 14}, {8291, 8009, 14},
			{8293, 8009, 14}, {8284, 8009, 14}, {8287, 7991, 14},
			{8297, 8009, 14}, {8283, 8007, 14}, {8299, 7991, 14},
			{8310, 7997, 14}, {8303, 8009, 14}, {8290, 8009, 14},
			{8306, 8009, 14}, {8283, 7995, 14}, {8283, 8000, 14},
			{8295, 8009, 14}, {8310, 8006, 14}, {8304, 8009, 14},
			{8295, 7991, 14}, {8292, 8009, 14}, {8309, 8009, 14},
			{8283, 8004, 14}, {8307, 7991, 14}, {8305, 7991, 14},
			{8283, 8001, 14}, {8284, 7991, 14}, {8297, 7991, 14},
			{8310, 7993, 14}, {8303, 7991, 14}, {8294, 8009, 14},
			{8287, 8009, 14}, {8283, 7991, 14}, {8283, 7997, 14},
			{8308, 7991, 14}, {8304, 7991, 14}, {8298, 7991, 14},
			{8310, 8000, 14}, {8310, 7994, 14}, {8294, 7991, 14},
		}, true},
	} {

		for _, coord := range test.tiles {
			if test.expire {
				if _, ok := tiles[coord]; !ok {
					t.Errorf("missing expire tile for %s %v", test.reason, coord)
				} else {
					delete(tiles, coord)
				}
			} else {
				if _, ok := tiles[coord]; ok {
					t.Errorf("found expire tile for %s %v", test.reason, coord)
				}
			}
		}
	}

	if len(tiles) > 0 {
		t.Errorf("found %d unexpected tiles", len(tiles))
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
