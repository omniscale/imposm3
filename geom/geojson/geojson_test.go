package geojson

import (
	"bytes"
	"math"
	"testing"
)

func TestParsePolygon(t *testing.T) {
	r := bytes.NewBufferString(`{"type": "Polygon", "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]}`)
	geoms, err := ParseGeoJson(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(geoms) != 1 {
		t.Fatal(geoms)
	}

	if math.Abs(geoms[0].Area()-100) > 0.00001 {
		t.Fatal(geoms[0].Area())
	}

	r = bytes.NewBufferString(`{"type": "Polygon", "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]], [[5, 5], [6, 5], [6, 6], [5, 6], [5, 5]]]}`)
	geoms, err = ParseGeoJson(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(geoms) != 1 {
		t.Fatal(geoms)
	}

	if math.Abs(geoms[0].Area()-99) > 0.00001 {
		t.Fatal(geoms[0].Area())
	}

}

func TestParseMultiPolygon(t *testing.T) {
	r := bytes.NewBufferString(`{"type": "MultiPolygon", "coordinates":
        [[[[0, 0], [10, 0], [10, 10], [0, 0]]],
        [[[0, 0], [10, 0], [10, 10], [0, 0]]]]
    }`)
	geoms, err := ParseGeoJson(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(geoms) != 2 {
		t.Fatal(geoms)
	}
}

func TestParseFeature(t *testing.T) {
	r := bytes.NewBufferString(`{"type": "Feature", "geometry": {
        "type": "Polygon", "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]
    }}`)
	geoms, err := ParseGeoJson(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(geoms) != 1 {
		t.Fatal(geoms)
	}
	if math.Abs(geoms[0].Area()-100) > 0.00001 {
		t.Fatal(geoms[0].Area())
	}
}

func TestParseFeatureCollection(t *testing.T) {
	r := bytes.NewBufferString(`{"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry":
            {"type": "Polygon", "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]}
        },
        {"type": "Feature", "geometry":
            {"type": "Polygon", "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]}
        }
    ]}`)
	geoms, err := ParseGeoJson(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(geoms) != 2 {
		t.Fatal(geoms)
	}
	if math.Abs(geoms[0].Area()-100) > 0.00001 {
		t.Fatal(geoms[0].Area())
	}
}
