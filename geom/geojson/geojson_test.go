package geojson

import (
	"bytes"
	"testing"
)

func TestParsePolygon(t *testing.T) {
	r := bytes.NewBufferString(`{"type": "Polygon", "coordinates": [[[8, 50], [11, 50], [11, 53], [8, 53], [8, 50]]]}`)
	features, err := ParseGeoJSON(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(features) != 1 {
		t.Fatal(features)
	}

	if len(features[0].Polygon[0]) != 5 {
		t.Fatal(features)
	}

	// ignore z values
	r = bytes.NewBufferString(`{"type": "Polygon", "coordinates": [[[8, 50, 0], [11, 50, 0], [11, 53, 0], [8, 53, 0], [8, 50, 0]]]}`)
	features, err = ParseGeoJSON(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(features) != 1 {
		t.Fatal(features)
	}

	if len(features[0].Polygon[0]) != 5 {
		t.Fatal(features)
	}

	if p := features[0].Polygon[0][0]; p.Long != 8.0 || p.Lat != 50 {
		t.Fatal(features)
	}

	// with hole
	r = bytes.NewBufferString(`{"type": "Polygon", "coordinates": [[[8, 50], [11, 50], [11, 53], [8, 53], [8, 50]], [[9, 51], [10, 51], [10, 52], [9, 52], [9, 51]]]}`)
	features, err = ParseGeoJSON(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(features) != 1 {
		t.Fatal(features)
	}

	if len(features[0].Polygon) != 2 {
		t.Fatal(features)
	}

}

func TestParseMultiPolygon(t *testing.T) {
	r := bytes.NewBufferString(`{"type": "MultiPolygon", "coordinates":
        [[[[8, 50], [11, 50], [11, 53], [8, 50]]],
        [[[8, 50], [11, 50], [11, 53], [8, 50]]]]
    }`)
	features, err := ParseGeoJSON(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(features) != 2 {
		t.Fatal(features)
	}
}

func TestParseFeature(t *testing.T) {
	r := bytes.NewBufferString(`{"type": "Feature", "geometry": {
        "type": "Polygon", "coordinates": [[[8, 50], [11, 50], [11, 53], [8, 53], [8, 50]]]
    }}`)
	features, err := ParseGeoJSON(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(features) != 1 {
		t.Fatal(features)
	}
	if len(features[0].Polygon[0]) != 5 {
		t.Fatal(features)
	}
}

func TestParseFeatureCollection(t *testing.T) {
	r := bytes.NewBufferString(`{"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry":
            {"type": "Polygon", "coordinates": [[[8, 50], [11, 50], [11, 53], [8, 53], [8, 50]]]}
        },
        {"type": "Feature", "geometry":
            {"type": "Polygon", "coordinates": [[[8, 50], [11, 50], [11, 53], [8, 53], [8, 50]]]}
        }
    ]}`)
	features, err := ParseGeoJSON(r)

	if err != nil {
		t.Fatal(err)
	}

	if len(features) != 2 {
		t.Fatal(features)
	}
	if len(features[0].Polygon[0]) != 5 {
		t.Fatal(features)
	}
	if len(features[1].Polygon[0]) != 5 {
		t.Fatal(features)
	}
}

func TestParseProperties(t *testing.T) {
	r := bytes.NewBufferString(`{"type": "FeatureCollection", "features": [
        {"type": "Feature", "properties": {"foo": "bar", "baz": 42}, "geometry":
            {"type": "Polygon", "coordinates": [[[8, 50], [11, 50], [11, 53], [8, 53], [8, 50]]]}
        },
        {"type": "Feature", "geometry":
            {"type": "Polygon", "coordinates": [[[8, 50], [11, 50], [11, 53], [8, 53], [8, 50]]]}
        }
    ]}`)
	features, err := ParseGeoJSON(r)

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

	if len(features[0].Polygon[0]) != 5 {
		t.Fatal(features)
	}
	if len(features[1].Polygon[0]) != 5 {
		t.Fatal(features)
	}
}
