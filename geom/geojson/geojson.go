package geojson

import (
	"encoding/json"
	"errors"
	"fmt"
	"imposm3/geom/geos"
	"imposm3/proj"
	"io"
)

type object struct {
	Type        string                 `json:"type"`
	Features    []object               `json:"features"`
	Geometry    *object                `json:"geometry"`
	Coordinates []interface{}          `json:"coordinates"`
	Properties  map[string]interface{} `json:"properties"`
}

type geometry struct {
	Type        string        `json:"type"`
	Coordinates []interface{} `json:"coordinates"`
}

type point struct {
	long float64
	lat  float64
}

func newPointFromCoords(coords []interface{}) (point, error) {
	p := point{}
	if len(coords) != 2 && len(coords) != 3 {
		return p, errors.New("point list length not 2 or 3")
	}
	var ok bool
	p.long, ok = coords[0].(float64)
	if !ok {
		return p, errors.New("invalid lon")
	}
	p.lat, ok = coords[1].(float64)
	if !ok {
		return p, errors.New("invalid lat")
	}

	if p.long >= -180.0 && p.long <= 180.0 && p.lat >= -90.0 && p.lat <= 90.0 {
		p.long, p.lat = proj.WgsToMerc(p.long, p.lat)
	}
	return p, nil
}

type lineString []point

func newLineStringFromCoords(coords []interface{}) (lineString, error) {
	ls := lineString{}

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

type polygon []lineString

type polygonFeature struct {
	polygon    polygon
	properties map[string]string
}

type Feature struct {
	Geom       *geos.Geom
	Properties map[string]string
}

func stringProperties(properties map[string]interface{}) map[string]string {
	result := make(map[string]string, len(properties))
	for k, v := range properties {
		result[k] = fmt.Sprintf("%v", v)
	}
	return result
}

func newPolygonFromCoords(coords []interface{}) (polygon, error) {
	poly := polygon{}

	for _, part := range coords {
		lsCoords, ok := part.([]interface{})
		if !ok {
			return poly, errors.New("polygon lineString not a list")
		}
		ls, err := newLineStringFromCoords(lsCoords)
		if err != nil {
			return poly, err
		}
		poly = append(poly, ls)
	}
	return poly, nil
}

func newMultiPolygonFeaturesFromCoords(coords []interface{}) ([]polygonFeature, error) {
	features := []polygonFeature{}

	for _, part := range coords {
		polyCoords, ok := part.([]interface{})
		if !ok {
			return features, errors.New("multipolygon polygon not a list")
		}
		poly, err := newPolygonFromCoords(polyCoords)
		if err != nil {
			return features, err
		}
		features = append(features, polygonFeature{poly, nil})
	}
	return features, nil
}

func ParseGeoJson(r io.Reader) ([]Feature, error) {
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

	g := geos.NewGeos()
	defer g.Finish()
	result := []Feature{}

	for _, p := range polygons {
		geom, err := geosPolygon(g, p.polygon)
		if err != nil {
			return nil, err
		}
		result = append(result, Feature{geom, p.properties})

	}
	return result, err
}

func constructPolygonFeatures(obj *object) ([]polygonFeature, error) {
	switch obj.Type {
	case "Point":
		return nil, errors.New("only polygon or MultiPolygon are supported")
	case "LineString":
		return nil, errors.New("only polygon or MultiPolygon are supported")
	case "Polygon":
		poly, err := newPolygonFromCoords(obj.Coordinates)
		return []polygonFeature{{poly, nil}}, err
	case "MultiPolygon":
		poly, err := newMultiPolygonFeaturesFromCoords(obj.Coordinates)
		return poly, err
	case "Feature":
		features, err := constructPolygonFeatures(obj.Geometry)
		if err != nil {
			return nil, err
		}
		properties := stringProperties(obj.Properties)
		for i, _ := range features {
			features[i].properties = properties
		}
		return features, err
	case "FeatureCollection":
		features := make([]polygonFeature, 0)

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

func geosRing(g *geos.Geos, ls lineString) (*geos.Geom, error) {
	coordSeq, err := g.CreateCoordSeq(uint32(len(ls)), 2)
	if err != nil {
		return nil, err
	}

	// coordSeq inherited by LinearRing, no destroy
	for i, p := range ls {
		err := coordSeq.SetXY(g, uint32(i), p.long, p.lat)
		if err != nil {
			return nil, err
		}
	}
	ring, err := coordSeq.AsLinearRing(g)
	if err != nil {
		// coordSeq gets Destroy by GEOS
		return nil, err
	}

	return ring, nil
}

func geosPolygon(g *geos.Geos, polygon polygon) (*geos.Geom, error) {
	if len(polygon) == 0 {
		return nil, errors.New("empty polygon")
	}

	shell, err := geosRing(g, polygon[0])
	if err != nil {
		return nil, err
	}

	holes := make([]*geos.Geom, len(polygon)-1)

	for i, ls := range polygon[1:] {
		hole, err := geosRing(g, ls)
		if err != nil {
			return nil, err
		}
		holes[i] = hole
	}

	geom := g.Polygon(shell, holes)
	if geom == nil {
		g.Destroy(shell)
		for _, hole := range holes {
			g.Destroy(hole)
		}
		return nil, errors.New("unable to create polygon")
	}
	return geom, nil
}
