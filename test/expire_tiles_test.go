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

func TestExpireTiles(t *testing.T) {
	if testing.Short() {
		t.Skip("system test skipped with -test.short")
	}
	t.Parallel()

	ts := importTestSuite{
		name: "expire_tiles",
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
	})

	t.Run("Import", func(t *testing.T) {
		if ts.tableExists(t, ts.dbschemaImport(), "osm_roads") != false {
			t.Fatalf("table osm_roads exists in schema %s", ts.dbschemaImport())
		}
		ts.importOsm(t)
		ts.deployOsm(t)
		if ts.tableExists(t, ts.dbschemaProduction(), "osm_roads") != true {
			t.Fatalf("table osm_roads does not exists in schema %s", ts.dbschemaProduction())
		}
	})

	t.Run("Elements", func(t *testing.T) {
		ts.assertRecords(t, []checkElem{
			{"osm_roads", 20151, "motorway", nil},
			{"osm_roads", 20251, "motorway", nil},
			{"osm_roads", 20351, "motorway", nil},
			{"osm_roads", 20651, "motorway", nil},

			{"osm_buildings", -30191, "yes", nil},
			{"osm_buildings", -30291, "yes", nil},
			{"osm_buildings", -30391, "yes", nil},
			{"osm_buildings", -30491, "yes", nil},
		})
	})

	t.Run("Update", func(t *testing.T) {
		ts.updateOsm(t, "build/expire_tiles.osc.gz")
	})

	t.Run("CheckExpireFile", func(t *testing.T) {
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
			{"delete way including its nodes", []tile{{8465, 8100, 14}}, true},

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

			{"create polygon (zoom out)", []tile{
				{2073, 2002, 12}, {2076, 1999, 12}, {2076, 2000, 12},
				{2073, 2001, 12}, {2070, 2000, 12}, {2070, 2002, 12},
				{2071, 2002, 12}, {2077, 1999, 12}, {2073, 1997, 12},
				{2074, 1999, 12}, {2075, 2001, 12}, {2077, 1997, 12},
				{2071, 1997, 12}, {2075, 1998, 12}, {2073, 2000, 12},
				{2076, 1998, 12}, {2074, 1998, 12}, {2071, 2000, 12},
				{2076, 1997, 12}, {2076, 2001, 12}, {2075, 2002, 12},
				{2072, 1997, 12}, {2076, 2002, 12}, {2070, 1998, 12},
				{2074, 2000, 12}, {2077, 2001, 12}, {2075, 1997, 12},
				{2074, 1997, 12}, {2071, 2001, 12}, {2075, 1999, 12},
				{2072, 1999, 12}, {2072, 2001, 12}, {2077, 2000, 12},
				{2073, 1999, 12}, {2077, 2002, 12}, {2072, 2000, 12},
				{2071, 1999, 12}, {2072, 1998, 12}, {2075, 2000, 12},
				{2072, 2002, 12}, {2071, 1998, 12}, {2073, 1998, 12},
				{2070, 1999, 12}, {2077, 1998, 12}, {2074, 2002, 12},
				{2074, 2001, 12}, {2070, 1997, 12}, {2070, 2001, 12},
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
		for tile := range tiles {
			t.Errorf("unexpected tile expired: %v", tile)
		}
	})

	t.Run("Cleanup", func(t *testing.T) {
		ts.dropSchemas()
		if err := os.RemoveAll(ts.dir); err != nil {
			t.Error(err)
		}
	})
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
