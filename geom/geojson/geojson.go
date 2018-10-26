package geojson

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/omniscale/imposm3/log"
)

type object struct {
	Type        string                 `json:"type"`
	Features    []object               `json:"features"`
	Geometry    *object                `json:"geometry"`
	Coordinates []interface{}          `json:"coordinates"`
	Properties  map[string]interface{} `json:"properties"`
}

type Point struct {
	Long float64
	Lat  float64
}

func newPointFromCoords(coords []interface{}) (Point, error) {
	p := Point{}
	if len(coords) != 2 && len(coords) != 3 {
		return p, errors.New("point list length not 2 or 3")
	}
	var ok bool
	p.Long, ok = coords[0].(float64)
	if !ok {
		return p, errors.New("invalid lon")
	}
	p.Lat, ok = coords[1].(float64)
	if !ok {
		return p, errors.New("invalid lat")
	}

	if p.Long > 180.0 || p.Long < -180.0 || p.Lat > 90.0 || p.Lat < -90.0 {
		log.Println("[warn] coordinates outside of world boundary. non-4326?: ", p)
	}

	return p, nil
}

type LineString []Point

func newLineStringFromCoords(coords []interface{}) (LineString, error) {
	ls := LineString{}

	for _, part := range coords {
		coord, ok := part.([]interface{})
		if !ok {
			return ls, errors.New("point not a list")
		}
		p, err := newPointFromCoords(coord)
		if err != nil {
			return ls, err
		}
		ls = append(ls, p)
	}
	return ls, nil
}

type Polygon []LineString

type Feature struct {
	Polygon    Polygon
	Properties map[string]string
}

func stringProperties(properties map[string]interface{}) map[string]string {
	result := make(map[string]string, len(properties))
	for k, v := range properties {
		result[k] = fmt.Sprintf("%v", v)
	}
	return result
}

func newPolygonFromCoords(coords []interface{}) (Polygon, error) {
	poly := Polygon{}

	for _, part := range coords {
		lsCoords, ok := part.([]interface{})
		if !ok {
			return poly, errors.New("polygon LineString not a list")
		}
		ls, err := newLineStringFromCoords(lsCoords)
		if err != nil {
			return poly, err
		}
		poly = append(poly, ls)
	}
	return poly, nil
}

func newMultiPolygonFeaturesFromCoords(coords []interface{}) ([]Feature, error) {
	features := []Feature{}

	for _, part := range coords {
		polyCoords, ok := part.([]interface{})
		if !ok {
			return features, errors.New("multipolygon polygon not a list")
		}
		poly, err := newPolygonFromCoords(polyCoords)
		if err != nil {
			return features, err
		}
		features = append(features, Feature{poly, nil})
	}
	return features, nil
}

// ParseGeoJSON parses geojson from reader and returns []Feature in WGS84 and
// another []Feature tranformed in targetSRID.
func ParseGeoJSON(r io.Reader) ([]Feature, error) {
	decoder := json.NewDecoder(r)

	obj := &object{}
	err := decoder.Decode(obj)
	if err != nil {
		return nil, err
	}

	polygons, err := constructPolygonFeatures(obj)

	if err != nil {
		return nil, err
	}

	return polygons, nil
}

func constructPolygonFeatures(obj *object) ([]Feature, error) {
	switch obj.Type {
	case "Point":
		return nil, errors.New("only polygon or MultiPolygon are supported")
	case "LineString":
		return nil, errors.New("only polygon or MultiPolygon are supported")
	case "Polygon":
		poly, err := newPolygonFromCoords(obj.Coordinates)
		return []Feature{{poly, nil}}, err
	case "MultiPolygon":
		poly, err := newMultiPolygonFeaturesFromCoords(obj.Coordinates)
		return poly, err
	case "Feature":
		features, err := constructPolygonFeatures(obj.Geometry)
		if err != nil {
			return nil, err
		}
		properties := stringProperties(obj.Properties)
		for i := range features {
			features[i].Properties = properties
		}
		return features, err
	case "FeatureCollection":
		features := make([]Feature, 0)

		for _, obj := range obj.Features {
			f, err := constructPolygonFeatures(&obj)
			if err != nil {
				return nil, err
			}
			features = append(features, f...)
		}
		return features, nil
	default:
		return nil, errors.New("unknown type: " + obj.Type)
	}
}
