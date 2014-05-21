package geojson

import (
	"bytes"
	"math"
	"testing"
)

func TestParsePolygon(t *testing.T) {
	r := bytes.NewBufferString(`{"type": "Polygon", "coordinates": [[[1000, 1000], [2000, 1000], [2000, 2000], [1000, 2000], [1000, 1000]]]}`)
	features, err := ParseGeoJson(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(features) != 1 {
		t.Fatal(features)
	}

	if math.Abs(features[0].Geom.Area()-1000000) > 0.00001 {
		t.Fatal(features[0].Geom.Area())
	}

	// ignore z values
	r = bytes.NewBufferString(`{"type": "Polygon", "coordinates": [[[1000, 1000, 1000], [2000, 1000, 1000], [2000, 2000, 1000], [1000, 2000, 1000], [1000, 1000, 1000]]]}`)
	features, err = ParseGeoJson(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(features) != 1 {
		t.Fatal(features)
	}

	if math.Abs(features[0].Geom.Area()-1000000) > 0.00001 {
		t.Fatal(features[0].Geom.Area())
	}

	r = bytes.NewBufferString(`{"type": "Polygon", "coordinates": [[[1000, 1000], [2000, 1000], [2000, 2000], [1000, 2000], [1000, 1000]], [[500, 500], [600, 500], [600, 600], [500, 600], [500, 500]]]}`)
	features, err = ParseGeoJson(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(features) != 1 {
		t.Fatal(features)
	}

	if math.Abs(features[0].Geom.Area()-990000) > 0.00001 {
		t.Fatal(features[0].Geom.Area())
	}

}

func TestParseMultiPolygon(t *testing.T) {
	r := bytes.NewBufferString(`{"type": "MultiPolygon", "coordinates":
        [[[[1000, 1000], [2000, 1000], [2000, 2000], [1000, 1000]]],
        [[[1000, 1000], [2000, 1000], [2000, 2000], [1000, 1000]]]]
    }`)
	features, err := ParseGeoJson(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(features) != 2 {
		t.Fatal(features)
	}
}

func TestParseFeature(t *testing.T) {
	r := bytes.NewBufferString(`{"type": "Feature", "geometry": {
        "type": "Polygon", "coordinates": [[[1000, 1000], [2000, 1000], [2000, 2000], [1000, 2000], [1000, 1000]]]
    }}`)
	features, err := ParseGeoJson(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(features) != 1 {
		t.Fatal(features)
	}
	if math.Abs(features[0].Geom.Area()-1000000) > 0.00001 {
		t.Fatal(features[0].Geom.Area())
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
	features, err := ParseGeoJson(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(features) != 2 {
		t.Fatal(features)
	}
	if math.Abs(features[0].Geom.Area()-1000000) > 0.00001 {
		t.Fatal(features[0].Geom.Area())
	}
}

func TestParseGeoJson(t *testing.T) {
	r := bytes.NewBufferString(`{"type": "FeatureCollection", "features": [
        {"type": "Feature", "properties": {"foo": "bar", "baz": 42}, "geometry":
            {"type": "Polygon", "coordinates": [[[1000, 1000], [2000, 1000], [2000, 2000], [1000, 2000], [1000, 1000]]]}
        },
        {"type": "Feature", "geometry":
            {"type": "Polygon", "coordinates": [[[1000, 1000], [2000, 1000], [2000, 2000], [1000, 2000], [1000, 1000]]]}
        }
    ]}`)
	features, err := ParseGeoJson(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(features) != 2 {
		t.Fatal(features)
	}
	if v, ok := features[0].Properties["foo"]; !ok || v != "bar" {
		t.Errorf("foo != bar, but '%v'", v)
	}
	if v, ok := features[0].Properties["baz"]; !ok || v != "42" {
		t.Errorf("baz != 42, but '%v'", v)
	}

	if math.Abs(features[0].Geom.Area()-1000000) > 0.00001 {
		t.Fatal(features[0].Geom.Area())
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
	features, err := ParseGeoJson(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(features) != 2 {
		t.Fatal(features)
	}
	if math.Abs(features[0].Geom.Area()-20834374847.98027) > 0.01 {
		t.Fatal(features[0].Geom.Area())
	}
}
