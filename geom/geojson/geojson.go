package geojson

import (
	"encoding/json"
	"errors"
	"fmt"
	"imposm3/geom/geos"
	"io"
	"log"
	"os"
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

type Point struct {
	Long float64
	Lat  float64
}

func newPointFromCoords(coords []interface{}) (Point, error) {
	p := Point{}
	if len(coords) != 2 {
		return p, errors.New("point list length not 2")
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
	return p, nil
}

type LineString struct {
	Points []Point
}

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
		ls.Points = append(ls.Points, p)
	}
	return ls, nil
}

type Polygon struct {
	LineStrings []LineString
}

func newPolygonFromCoords(coords []interface{}) (Polygon, error) {
	poly := Polygon{}

	for _, part := range coords {
		lsCoords, ok := part.([]interface{})
		if !ok {
			return poly, errors.New("polygon linestring not a list")
		}
		ls, err := newLineStringFromCoords(lsCoords)
		if err != nil {
			return poly, err
		}
		poly.LineStrings = append(poly.LineStrings, ls)
	}
	return poly, nil
}

func newMultiPolygonFromCoords(coords []interface{}) ([]Polygon, error) {
	mp := []Polygon{}

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

func ParseGeoJson(geojson string) (interface{}, error) {
	obj := &object{}
	err := json.Unmarshal([]byte(geojson), obj)

	if err != nil {
		return nil, err
	}

	return constructPolygons(obj)
}

func ParseGeoJsonReader(r io.Reader) (interface{}, error) {
	decoder := json.NewDecoder(r)

	obj := &object{}
	err := decoder.Decode(obj)
	if err != nil {
		return nil, err
	}

	return constructPolygons(obj)
}

func newFeatureFromObj(obj *object) (interface{}, error) {
	switch obj.Geometry.Type {
	case "Point":
		p, err := newPointFromCoords(obj.Geometry.Coordinates)
		return p, err
	case "LineString":
		ls, err := newLineStringFromCoords(obj.Geometry.Coordinates)
		return ls, err
	case "Polygon":
		poly, err := newPolygonFromCoords(obj.Geometry.Coordinates)
		return poly, err
	case "MultiPolygon":
		poly, err := newMultiPolygonFromCoords(obj.Geometry.Coordinates)
		return poly, err
	default:
		return nil, errors.New("unknown geometry type: " + obj.Geometry.Type)
	}
	return nil, nil
}

func constructPolygons(obj *object) ([]Polygon, error) {
	switch obj.Type {
	case "Point":
		return nil, errors.New("only Polygon or MultiPolygon are supported")
	case "LineString":
		return nil, errors.New("only Polygon or MultiPolygon are supported")
	case "Polygon":
		poly, err := newPolygonFromCoords(obj.Coordinates)
		return []Polygon{poly}, err
	case "MultiPolygon":
		poly, err := newMultiPolygonFromCoords(obj.Coordinates)
		return poly, err
	case "Feature":
		geom, err := constructPolygons(obj.Geometry)
		return geom, err
	case "FeatureCollection":
		features := make([]Polygon, 0)

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

func geosRing(g *geos.Geos, ls LineString) (*geos.Geom, error) {
	coordSeq, err := g.CreateCoordSeq(uint32(len(ls.Points)), 2)
	if err != nil {
		return nil, err
	}

	// coordSeq inherited by LinearRing, no destroy
	for i, p := range ls.Points {
		err := coordSeq.SetXY(g, uint32(i), p.Long, p.Lat)
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

func geosPolygon(g *geos.Geos, polygon Polygon) (*geos.Geom, error) {
	shell, err := geosRing(g, polygon.LineStrings[0])
	if err != nil {
		return nil, err
	}

	holes := make([]*geos.Geom, len(polygon.LineStrings)-1)

	for i, ls := range polygon.LineStrings[1:] {
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
	g.DestroyLater(geom)
	return geom, nil
}

var tests = []string{
	// `{"type": "Point", "coordinates": [102.0, 0.5]}`,
	// `{"type": "LineString", "coordinates": [[102.1, 0.0], [103.0, 1.0], [104.0, 0.0], [105.0, 1.0]]}`,
	`{"type": "Polygon", "coordinates": [[[102.1, 0.0], [103.0, 1.0], [104.0, 0.0], [105.0, 1.0]], [[0, 0]]]}`,
	`{"type": "MultiPolygon", "coordinates": [[[[102.1, 0.0], [103.0, 1.0], [104.0, 0.0], [105.0, 1.0]], [[0, 0]]]]}`,
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	for _, test := range tests {
		fmt.Println("parsing: ", test)
		geo, err := ParseGeoJson(test)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(geo)
	}

	fmt.Println("parsing file")
	f, err := os.Open("/Users/olt/dev/cust/server/mapnik/conf/imposm/polygons/germany_clip_boundary_3857.geojson")
	// f, err := os.Open("/Users/olt/dev/cust/server/mapnik/conf/imposm/polygons/germany_buffer.geojson")
	if err != nil {
		log.Fatal(err)
	}

	gj, err := NewGeoJson(f)
	if err != nil {
		log.Fatal(err)
	}
	polygons, err := gj.Geoms()
	if err != nil {
		log.Fatal(err)
	}
	for _, p := range polygons {
		fmt.Println(p.Area())
	}
	// fmt.Println(geo)

}
