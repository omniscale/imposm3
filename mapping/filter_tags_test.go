package mapping

import (
	"io/ioutil"
	"os"
	"testing"

	osm "github.com/omniscale/go-osm"
)

func TestFilters_require(t *testing.T) {
	filterTest(
		t,
		`
tables:
  admin:
    fields:
    - name: id
      type: id
    - key: admin_level
      name: admin_level
      type: integer
    filters:
      require:
        boundary: ["administrative","maritime"]
    mapping:
      admin_level: ['2','4']
    type: linestring
`,
		// Accept
		[]osm.Tags{
			osm.Tags{"admin_level": "2", "boundary": "administrative"},
			osm.Tags{"admin_level": "2", "boundary": "maritime"},
			osm.Tags{"admin_level": "4", "boundary": "administrative", "name": "N4"},
			osm.Tags{"admin_level": "4", "boundary": "maritime", "name": "N4"},
		},
		// Reject
		[]osm.Tags{
			osm.Tags{"admin_level": "0", "boundary": "administrative"},
			osm.Tags{"admin_level": "1", "boundary": "administrative"},
			osm.Tags{"admin_level": "2", "boundary": "postal_code"},
			osm.Tags{"admin_level": "2", "boundary": ""},
			osm.Tags{"admin_level": "2", "boundary": "__nil__"},
			osm.Tags{"admin_level": "4", "boundary": "census"},
			osm.Tags{"admin_level": "3", "boundary": "administrative", "name": "NX"},
			osm.Tags{"admin_level": "2"},
			osm.Tags{"admin_level": "4"},
			osm.Tags{"admin_level": "❤"},
			osm.Tags{"admin_level": "__any__", "boundary": "__any__"},
			osm.Tags{"boundary": "administrative"},
			osm.Tags{"boundary": "maritime"},
			osm.Tags{"name": "maritime"},
		},
	)
}

func TestFilters_require2(t *testing.T) {
	// same as above, but mapping and filters are swapped
	filterTest(
		t,
		`
tables:
  admin:
    fields:
    - name: id
      type: id
    - key: admin_level
      name: admin_level
      type: integer
    filters:
      require:
        admin_level: ["2","4"]
    mapping:
      boundary:
      - administrative
      - maritime
    type: linestring
`,
		// Accept
		[]osm.Tags{
			osm.Tags{"admin_level": "2", "boundary": "administrative"},
			osm.Tags{"admin_level": "2", "boundary": "maritime"},
			osm.Tags{"admin_level": "4", "boundary": "administrative", "name": "N4"},
			osm.Tags{"admin_level": "4", "boundary": "maritime", "name": "N4"},
		},
		// Reject
		[]osm.Tags{
			osm.Tags{"admin_level": "0", "boundary": "administrative"},
			osm.Tags{"admin_level": "1", "boundary": "administrative"},
			osm.Tags{"admin_level": "2", "boundary": "postal_code"},
			osm.Tags{"admin_level": "2", "boundary": ""},
			osm.Tags{"admin_level": "2", "boundary": "__nil__"},
			osm.Tags{"admin_level": "4", "boundary": "census"},
			osm.Tags{"admin_level": "3", "boundary": "administrative", "name": "NX"},
			osm.Tags{"admin_level": "2"},
			osm.Tags{"admin_level": "4"},
			osm.Tags{"admin_level": "❤"},
			osm.Tags{"admin_level": "__any__", "boundary": "__any__"},
			osm.Tags{"boundary": "administrative"},
			osm.Tags{"boundary": "maritime"},
			osm.Tags{"name": "maritime"},
		},
	)
}
func TestFilters_building(t *testing.T) {

	filterTest(
		t,
		`
tables:
  buildings:
    fields:
    - name: id
      type: id
    - key: building
      name: building
      type: string
    filters:
      reject:
        building: ["no","none"]
      require_regexp:
        'addr:housenumber': '^\d+[a-zA-Z,]*$'
        building: '^[a-z_]+$'
    mapping:
      building:
      - __any__
    type: linestring
`,
		// Accept
		[]osm.Tags{
			osm.Tags{"building": "yes", "addr:housenumber": "1a"},
			osm.Tags{"building": "house", "addr:housenumber": "131"},
			osm.Tags{"building": "residential", "addr:housenumber": "21"},
			osm.Tags{"building": "garage", "addr:housenumber": "0"},
			osm.Tags{"building": "hut", "addr:housenumber": "99999999"},
			osm.Tags{"building": "_", "addr:housenumber": "333"},

			osm.Tags{"building": "__any__", "addr:housenumber": "333"},
			osm.Tags{"building": "__nil__", "addr:housenumber": "333"},
			osm.Tags{"building": "y", "addr:housenumber": "1abcdefg"},
			osm.Tags{"building": "tower_block", "addr:housenumber": "1A"},
			osm.Tags{"building": "shed", "name": "N4", "addr:housenumber": "1AAA"},
			osm.Tags{"building": "office", "name": "N4", "addr:housenumber": "0XYAB,"},
		},
		// Reject
		[]osm.Tags{
			osm.Tags{"building": "yes", "addr:housenumber": "aaaaa-number"},
			osm.Tags{"building": "house", "addr:housenumber": "1-3a"},
			osm.Tags{"building": "house", "addr:housenumber": "❤"},
			osm.Tags{"building": "house", "addr:housenumber": "two"},
			osm.Tags{"building": "residential", "addr:housenumber": "x21"},

			osm.Tags{"building": "", "addr:housenumber": "111"},

			osm.Tags{"building": "no"},
			osm.Tags{"building": "no", "addr:housenumber": "1a"},
			osm.Tags{"building": "No", "addr:housenumber": "1a"},
			osm.Tags{"building": "NO", "addr:housenumber": "1a"},
			osm.Tags{"building": "none"},
			osm.Tags{"building": "none", "addr:housenumber": "0"},
			osm.Tags{"building": "nONe", "addr:housenumber": "0"},
			osm.Tags{"building": "No"},
			osm.Tags{"building": "NO"},
			osm.Tags{"building": "NONe"},
			osm.Tags{"building": "Garage"},
			osm.Tags{"building": "Hut"},
			osm.Tags{"building": "Farm"},
			osm.Tags{"building": "tower-block"},
			osm.Tags{"building": "❤"},
			osm.Tags{"building": "Ümlåütê"},
			osm.Tags{"building": "木"},
			osm.Tags{"building": "SheD", "name": "N4"},
			osm.Tags{"building": "oFFice", "name": "N4"},
			osm.Tags{"admin_level": "2"},
			osm.Tags{"admin_level": "4"},
			osm.Tags{"boundary": "administrative"},
			osm.Tags{"boundary": "maritime"},
			osm.Tags{"name": "maritime"},
		},
	)
}

func TestFilters_highway_with_name(t *testing.T) {
	filterTest(
		t,
		`
tables:
  highway:
    fields:
    - name: id
      type: id
    - key: highway
      name: highway
      type: string
    - key: name
      name: name
      type: string
    filters:
      require:
        name: ["__any__"]
      reject:
        highway: ["no","none"]
    mapping:
      highway:
      - __any__
    type: linestring
`,
		// Accept
		[]osm.Tags{
			osm.Tags{"highway": "residential", "name": "N1"},
			osm.Tags{"highway": "service", "name": "N2"},
			osm.Tags{"highway": "track", "name": "N3"},
			osm.Tags{"highway": "unclassified", "name": "N4"},
			osm.Tags{"highway": "path", "name": "N5"},
			osm.Tags{"highway": "", "name": "🌍🌎🌏"},
			osm.Tags{"highway": "_", "name": "N6"},
			osm.Tags{"highway": "y", "name": "N7"},
			osm.Tags{"highway": "tower_block", "name": "N8"},
			osm.Tags{"highway": "shed", "name": "N9"},
			osm.Tags{"highway": "office", "name": "N10"},
			osm.Tags{"highway": "SheD", "name": "N11"},
			osm.Tags{"highway": "oFFice", "name": "N12"},
			osm.Tags{"highway": "❤", "name": "❤"},
			osm.Tags{"highway": "Ümlåütê", "name": "Ümlåütê"},
			osm.Tags{"highway": "木", "name": "木"},
		},
		// Reject
		[]osm.Tags{
			osm.Tags{"highway": "no", "name": "N1"},
			osm.Tags{"highway": "none", "name": "N2"},
			osm.Tags{"highway": "yes"},
			osm.Tags{"highway": "no"},
			osm.Tags{"highway": "none"},
			osm.Tags{"highway": "No"},
			osm.Tags{"highway": "NO"},
			osm.Tags{"highway": "NONe"},
			osm.Tags{"highway": "Garage"},
			osm.Tags{"highway": "residential"},
			osm.Tags{"highway": "path"},
			osm.Tags{"highway": "tower-block"},
			osm.Tags{"highway": "❤"},
			osm.Tags{"highway": "Ümlåütê"},
			osm.Tags{"highway": "木"},
			osm.Tags{"admin_level": "2"},
			osm.Tags{"admin_level": "4"},
			osm.Tags{"boundary": "administrative"},
			osm.Tags{"boundary": "maritime"},
			osm.Tags{"name": "maritime"},
		},
	)
}

func TestFilters_waterway_with_name(t *testing.T) {
	filterTest(
		t,
		`
tables:
  waterway:
    fields:
    - name: id
      type: id
    - key: waterway
      name: waterway
      type: string
    - key: name
      name: name
      type: string
    filters:
      require:
        name: ["__any__"]
        waterway:
        - stream
        - river
        - canal
        - drain
        - ditch
      reject:
        fixme: ['__any__']
        amenity: ['__any__']
        shop: ['__any__']
        building: ['__any__']
        tunnel: ['yes']
      reject_regexp:
        level: '^\D+.*$'
    mapping:
      waterway:
      - __any__
    type: linestring
`,
		// Accept
		[]osm.Tags{
			osm.Tags{"waterway": "stream", "name": "N1"},
			osm.Tags{"waterway": "river", "name": "N2"},
			osm.Tags{"waterway": "canal", "name": "N3"},
			osm.Tags{"waterway": "drain", "name": "N4"},
			osm.Tags{"waterway": "ditch", "name": "N5"},

			osm.Tags{"waterway": "stream", "name": "N1", "tunnel": "no"},
			osm.Tags{"waterway": "river", "name": "N2", "boat": "no"},
			osm.Tags{"waterway": "canal", "name": "N3"},
			osm.Tags{"waterway": "ditch", "name": "N4", "level": "3"},

			osm.Tags{"waterway": "stream", "name": "__any__"},
			osm.Tags{"waterway": "stream", "name": "__nil__"},

			osm.Tags{"waterway": "stream", "name": "❤"},
			osm.Tags{"waterway": "stream", "name": "木"},
			osm.Tags{"waterway": "stream", "name": "Ümlåütê"},
		},
		// Reject
		[]osm.Tags{
			osm.Tags{"waterway": "ditch", "name": "N1", "fixme": "incomplete"},
			osm.Tags{"waterway": "stream", "name": "N1", "amenity": "parking"},
			osm.Tags{"waterway": "river", "name": "N2", "shop": "hairdresser"},
			osm.Tags{"waterway": "canal", "name": "N3", "building": "house"},
			osm.Tags{"waterway": "drain", "name": "N1 tunnel", "tunnel": "yes"},

			osm.Tags{"waterway": "river", "name": "N4", "level": "unknown"},
			osm.Tags{"waterway": "ditch", "name": "N4", "level": "primary"},

			osm.Tags{"waterway": "path", "name": "N5"},
			osm.Tags{"waterway": "_", "name": "N6"},
			osm.Tags{"waterway": "y", "name": "N7"},
			osm.Tags{"waterway": "tower_block", "name": "N8"},
			osm.Tags{"waterway": "shed", "name": "N9"},
			osm.Tags{"waterway": "office", "name": "N10"},
			osm.Tags{"waterway": "SheD", "name": "N11"},
			osm.Tags{"waterway": "oFFice", "name": "N12"},
			osm.Tags{"waterway": "❤", "name": "❤"},
			osm.Tags{"waterway": "Ümlåütê", "name": "Ümlåütê"},
			osm.Tags{"waterway": "木", "name": "木"},
			osm.Tags{"waterway": "no", "name": "N1"},
			osm.Tags{"waterway": "none", "name": "N2"},

			osm.Tags{"waterway": "yes"},
			osm.Tags{"waterway": "no"},
			osm.Tags{"waterway": "none"},
			osm.Tags{"waterway": "tower-block"},
			osm.Tags{"waterway": "❤"},
			osm.Tags{"waterway": "Ümlåütê"},
			osm.Tags{"waterway": "木"},

			osm.Tags{"waterway": "__nil__", "name": "__nil__"},
			osm.Tags{"waterway": "__any__", "name": "__nil__"},

			osm.Tags{"waterway": "stream", "name": "__any__", "shop": "__any__"},
			osm.Tags{"waterway": "stream", "name": "__nil__", "shop": "__any__"},
			osm.Tags{"waterway": "stream", "name": "__any__", "shop": "__nil__"},
			osm.Tags{"waterway": "stream", "name": "__nil__", "shop": "__nil__"},
			osm.Tags{"waterway": "stream", "name": "__any__", "shop": ""},
			osm.Tags{"waterway": "stream", "name": "__nil__", "shop": ""},

			osm.Tags{"admin_level": "2"},
			osm.Tags{"admin_level": "4"},
			osm.Tags{"boundary": "administrative"},
			osm.Tags{"boundary": "maritime"},
			osm.Tags{"name": "maritime"},
		},
	)
}

func TestFilters_exclude_tags(t *testing.T) {
	filterTest(
		t,
		`
tables:
  exclude_tags:
    _comment:  Allways Empty !
    fields:
    - name: id
      type: id
    - key: waterway
      name: waterway
      type: string
    - key: name
      name: name
      type: string
    filters:
      require:
        waterway:
         - stream
      exclude_tags:
      - ['waterway', 'river']
      - ['waterway', 'canal']
      - ['waterway', 'drain']
      - ['waterway', 'ditch']
    mapping:
      waterway:
      - __any__
    type: linestring
`,
		// Accept
		[]osm.Tags{
			osm.Tags{"waterway": "stream", "name": "N1"},
			osm.Tags{"waterway": "stream", "name": "N1", "tunnel": "no"},
			osm.Tags{"waterway": "stream", "name": "N1", "amenity": "parking"},
		},
		// Reject
		[]osm.Tags{
			osm.Tags{"waterway": "river", "name": "N2"},
			osm.Tags{"waterway": "canal", "name": "N3"},
			osm.Tags{"waterway": "drain", "name": "N4"},
			osm.Tags{"waterway": "ditch", "name": "N5"},

			osm.Tags{"waterway": "river", "name": "N2", "boat": "no"},
			osm.Tags{"waterway": "canal", "name": "N3"},
			osm.Tags{"waterway": "ditch", "name": "N4", "level": "3"},

			osm.Tags{"waterway": "ditch", "name": "N1", "fixme": "incomplete"},
			osm.Tags{"waterway": "river", "name": "N2", "shop": "hairdresser"},
			osm.Tags{"waterway": "canal", "name": "N3", "building": "house"},
			osm.Tags{"waterway": "drain", "name": "N1 tunnel", "tunnel": "yes"},

			osm.Tags{"waterway": "river", "name": "N4", "level": "unknown"},
			osm.Tags{"waterway": "ditch", "name": "N4", "level": "primary"},

			osm.Tags{"waterway": "path", "name": "N5"},
			osm.Tags{"waterway": "_", "name": "N6"},
			osm.Tags{"waterway": "y", "name": "N7"},
			osm.Tags{"waterway": "tower_block", "name": "N8"},
			osm.Tags{"waterway": "shed", "name": "N9"},
			osm.Tags{"waterway": "office", "name": "N10"},
			osm.Tags{"waterway": "SheD", "name": "N11"},
			osm.Tags{"waterway": "oFFice", "name": "N12"},
			osm.Tags{"waterway": "❤", "name": "❤"},
			osm.Tags{"waterway": "Ümlåütê", "name": "Ümlåütê"},
			osm.Tags{"waterway": "木", "name": "木"},

			osm.Tags{"waterway": "no", "name": "N1"},
			osm.Tags{"waterway": "none", "name": "N2"},
			osm.Tags{"waterway": "yes"},
			osm.Tags{"waterway": "no"},
			osm.Tags{"waterway": "none"},
			osm.Tags{"waterway": "tower-block"},
			osm.Tags{"waterway": "❤"},
			osm.Tags{"waterway": "Ümlåütê"},
			osm.Tags{"waterway": "木"},
			osm.Tags{"admin_level": "2"},
			osm.Tags{"admin_level": "4"},
			osm.Tags{"boundary": "administrative"},
			osm.Tags{"boundary": "maritime"},
			osm.Tags{"name": "maritime"},
		},
	)
}

func filterTest(t *testing.T, mapping string, accept []osm.Tags, reject []osm.Tags) {
	var configTestMapping *Mapping
	var err error

	tmpfile, err := ioutil.TempFile("", "filter_test_mapping.yml")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(mapping)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	configTestMapping, err = FromFile(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}

	var actualMatch []Match

	elem := osm.Way{}
	ls := configTestMapping.LineStringMatcher

	for _, et := range accept {
		elem.Tags = et
		actualMatch = ls.MatchWay(&elem)
		if len(actualMatch) == 0 {
			t.Errorf("TestFilter - Not Accepted : (%+v)  ", et)
		}
	}

	for _, et := range reject {
		elem.Tags = et
		actualMatch = ls.MatchWay(&elem)

		if len(actualMatch) != 0 {
			t.Errorf("TestFilter - Not Rejected : (%+v)  ", et)
		}
	}

}
