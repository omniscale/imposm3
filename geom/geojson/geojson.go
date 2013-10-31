package geojson

import (
	"encoding/json"
	"errors"
	"imposm3/geom/geos"
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
	if len(coords) != 2 {
		return p, errors.New("point list length not 2")
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

func newMultiPolygonFromCoords(coords []interface{}) ([]polygon, error) {
	mp := []polygon{}

	for _, part := range coords {
		polyCoords, ok := part.([]interface{})
		if !ok {
			return mp, errors.New("multipolygon polygon not a list")
		}
		poly, err := newPolygonFromCoords(polyCoords)
		if err != nil {
			return mp, err
		}
		mp = append(mp, poly)
	}
	return mp, nil
}

func ParseGeoJson(r io.Reader) ([]*geos.Geom, error) {
	decoder := json.NewDecoder(r)

	obj := &object{}
	err := decoder.Decode(obj)
	if err != nil {
		return nil, err
	}

	polygons, err := constructPolygons(obj)

	if err != nil {
		return nil, err
	}

	g := geos.NewGeos()
	defer g.Finish()
	result := []*geos.Geom{}

	for _, p := range polygons {
		geom, err := geosPolygon(g, p)
		if err != nil {
			return nil, err
		}
		result = append(result, geom)

	}
	return result, err
}

func constructPolygons(obj *object) ([]polygon, error) {
	switch obj.Type {
	case "Point":
		return nil, errors.New("only polygon or MultiPolygon are supported")
	case "LineString":
		return nil, errors.New("only polygon or MultiPolygon are supported")
	case "Polygon":
		poly, err := newPolygonFromCoords(obj.Coordinates)
		return []polygon{poly}, err
	case "MultiPolygon":
		poly, err := newMultiPolygonFromCoords(obj.Coordinates)
		return poly, err
	case "Feature":
		geom, err := constructPolygons(obj.Geometry)
		return geom, err
	case "FeatureCollection":
		features := make([]polygon, 0)

		for _, obj := range obj.Features {
			geom, err := constructPolygons(&obj)
			if err != nil {
				return nil, err
			}
			features = append(features, geom...)
		}
		return features, nil
	default:
		return nil, errors.New("unknown type: " + obj.Type)
	}
	return nil, nil
}

type GeoJson struct {
	object object
}

func NewGeoJson(r io.Reader) (*GeoJson, error) {
	result := &GeoJson{}

	decoder := json.NewDecoder(r)

	err := decoder.Decode(&result.object)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (gj *GeoJson) Geoms() ([]*geos.Geom, error) {

	polygons, err := constructPolygons(&gj.object)
	if err != nil {
		return nil, err
	}

	g := geos.NewGeos()
	defer g.Finish()
	result := []*geos.Geom{}

	for _, p := range polygons {
		geom, err := geosPolygon(g, p)
		if err != nil {
			return nil, err
		}
		result = append(result, geom)

	}
	return result, err
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
