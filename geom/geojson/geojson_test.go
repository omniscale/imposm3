package geojson

import (
	"bytes"
	"math"
	"testing"
)

func TestParsePolygon(t *testing.T) {
	r := bytes.NewBufferString(`{"type": "Polygon", "coordinates": [[[1000, 1000], [2000, 1000], [2000, 2000], [1000, 2000], [1000, 1000]]]}`)
	geoms, err := ParseGeoJson(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(geoms) != 1 {
		t.Fatal(geoms)
	}

	if math.Abs(geoms[0].Area()-1000000) > 0.00001 {
		t.Fatal(geoms[0].Area())
	}

	// ignore z values
	r = bytes.NewBufferString(`{"type": "Polygon", "coordinates": [[[1000, 1000, 1000], [2000, 1000, 1000], [2000, 2000, 1000], [1000, 2000, 1000], [1000, 1000, 1000]]]}`)
	geoms, err = ParseGeoJson(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(geoms) != 1 {
		t.Fatal(geoms)
	}

	if math.Abs(geoms[0].Area()-1000000) > 0.00001 {
		t.Fatal(geoms[0].Area())
	}

	r = bytes.NewBufferString(`{"type": "Polygon", "coordinates": [[[1000, 1000], [2000, 1000], [2000, 2000], [1000, 2000], [1000, 1000]], [[500, 500], [600, 500], [600, 600], [500, 600], [500, 500]]]}`)
	geoms, err = ParseGeoJson(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(geoms) != 1 {
		t.Fatal(geoms)
	}

	if math.Abs(geoms[0].Area()-990000) > 0.00001 {
		t.Fatal(geoms[0].Area())
	}

}

func TestParseMultiPolygon(t *testing.T) {
	r := bytes.NewBufferString(`{"type": "MultiPolygon", "coordinates":
        [[[[1000, 1000], [2000, 1000], [2000, 2000], [1000, 1000]]],
        [[[1000, 1000], [2000, 1000], [2000, 2000], [1000, 1000]]]]
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
        "type": "Polygon", "coordinates": [[[1000, 1000], [2000, 1000], [2000, 2000], [1000, 2000], [1000, 1000]]]
    }}`)
	geoms, err := ParseGeoJson(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(geoms) != 1 {
		t.Fatal(geoms)
	}
	if math.Abs(geoms[0].Area()-1000000) > 0.00001 {
		t.Fatal(geoms[0].Area())
	}
}

func TestParseFeatureCollection(t *testing.T) {
	r := bytes.NewBufferString(`{"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry":
            {"type": "Polygon", "coordinates": [[[1000, 1000], [2000, 1000], [2000, 2000], [1000, 2000], [1000, 1000]]]}
        },
        {"type": "Feature", "geometry":
            {"type": "Polygon", "coordinates": [[[1000, 1000], [2000, 1000], [2000, 2000], [1000, 2000], [1000, 1000]]]}
        }
    ]}`)
	geoms, err := ParseGeoJson(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(geoms) != 2 {
		t.Fatal(geoms)
	}
	if math.Abs(geoms[0].Area()-1000000) > 0.00001 {
		t.Fatal(geoms[0].Area())
	}
}

func TestParseGeoJson(t *testing.T) {
	r := bytes.NewBufferString(`{"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry":
            {"type": "Polygon", "coordinates": [[[1000, 1000], [2000, 1000], [2000, 2000], [1000, 2000], [1000, 1000]]]}
        },
        {"type": "Feature", "geometry":
            {"type": "Polygon", "coordinates": [[[1000, 1000], [2000, 1000], [2000, 2000], [1000, 2000], [1000, 1000]]]}
        }
    ]}`)
	geoms, err := ParseGeoJson(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(geoms) != 2 {
		t.Fatal(geoms)
	}
	if math.Abs(geoms[0].Area()-1000000) > 0.00001 {
		t.Fatal(geoms[0].Area())
	}
}

func TestParseGeoJsonTransform(t *testing.T) {
	// automatically transforms WGS84 to webmercator
	r := bytes.NewBufferString(`{"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry":
            {"type": "Polygon", "coordinates": [[[8, 53], [9, 53], [9, 54], [8, 54], [8, 53]]]}
        },
        {"type": "Feature", "geometry":
            {"type": "Polygon", "coordinates": [[[9, 53], [10, 53], [10, 54], [9, 54], [9, 53]]]}
        }
    ]}`)
	geoms, err := ParseGeoJson(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(geoms) != 2 {
		t.Fatal(geoms)
	}
	if math.Abs(geoms[0].Area()-20834374847.98027) > 0.01 {
		t.Fatal(geoms[0].Area())
	}
}
