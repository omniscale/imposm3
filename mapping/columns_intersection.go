package mapping

import (
	"errors"
	"os"
	"sync"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/geom/geojson"
	"github.com/omniscale/imposm3/geom/geos"
	"github.com/omniscale/imposm3/mapping/config"
	"github.com/omniscale/imposm3/proj"
)

type syncedPreparedGeom struct {
	sync.Mutex
	geom *geos.PreparedGeom
}

type feature struct {
	geom       *geos.Geom
	properties map[string]string
}

func loadFeatures(field config.Column) (*geos.Index, []feature, []syncedPreparedGeom, error) {
	_geojsonFileName, ok := field.Args["geojson"]
	if !ok {
		return nil, nil, nil, errors.New("missing geojson in args for geojson_feature_intersections")
	}
	geojsonFileName, ok := _geojsonFileName.(string)
	if !ok {
		return nil, nil, nil, errors.New("geojson in args for geojson_feature_intersections not a string")
	}

	g := geos.NewGeos()
	defer g.Finish()

	idx := g.CreateIndex()

	f, err := os.Open(geojsonFileName)
	if err != nil {
		return nil, nil, nil, err
	}
	defer f.Close()

	jsonFeatures, err := geojson.ParseGeoJSON(f)
	if err != nil {
		return nil, nil, nil, err
	}
	preparedGeoms := make([]syncedPreparedGeom, len(jsonFeatures))

	features := make([]feature, len(jsonFeatures))

	for i, f := range jsonFeatures {
		// TODO make SRID configurable
		transformPolygon(f.Polygon, 3857)
		geom, err := geosPolygon(g, f.Polygon)
		if err != nil {
			return nil, nil, nil, err
		}

		g.IndexAdd(idx, geom)
		preparedGeoms[i] = syncedPreparedGeom{geom: g.Prepare(geom)}
		features[i] = feature{geom: geom, properties: f.Properties}
	}
	return idx, features, preparedGeoms, nil
}

func MakeIntersectsFeatureField(fieldName string, fieldType ColumnType, field config.Column) (MakeValue, error) {
	idx, features, preparedGeoms, err := loadFeatures(field)
	if err != nil {
		return nil, err
	}

	_propertyName, ok := field.Args["property"]
	if !ok {
		return nil, errors.New("missing property in args for geojson_intersects_feature")
	}
	propertyName, ok := _propertyName.(string)
	if !ok {
		return nil, errors.New("property in args for geojson_intersects_feature not a string")
	}

	g := geos.NewGeos()

	makeValue := func(val string, elem *osm.Element, geom *geom.Geometry, m Match) interface{} {
		indices := g.IndexQuery(idx, geom.Geom)

		for _, idx := range indices {
			preparedGeom := &preparedGeoms[idx]
			preparedGeom.Lock()
			if g.PreparedIntersects(preparedGeom.geom, geom.Geom) {
				if v, ok := features[idx].properties[propertyName]; ok {
					preparedGeom.Unlock()
					return v
				}
			}
			preparedGeom.Unlock()
		}
		return nil
	}

	return makeValue, nil
}

func MakeIntersectsField(fieldName string, fieldType ColumnType, field config.Column) (MakeValue, error) {
	idx, _, preparedGeoms, err := loadFeatures(field)
	if err != nil {
		return nil, err
	}

	g := geos.NewGeos()

	makeValue := func(val string, elem *osm.Element, geom *geom.Geometry, m Match) interface{} {
		indices := g.IndexQuery(idx, geom.Geom)

		for _, idx := range indices {
			preparedGeom := &preparedGeoms[idx]
			preparedGeom.Lock()
			if g.PreparedIntersects(preparedGeom.geom, geom.Geom) {
				preparedGeom.Unlock()
				return true
			}
			preparedGeom.Unlock()
		}
		return false
	}

	return makeValue, nil
}

// TODO duplicate of imposm3/geom/limit
func geosRing(g *geos.Geos, ls geojson.LineString) (*geos.Geom, error) {
	coordSeq, err := g.CreateCoordSeq(uint32(len(ls)), 2)
	if err != nil {
		return nil, err
	}

	// coordSeq inherited by LinearRing, no destroy
	for i, p := range ls {
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

// TODO duplicate of imposm3/geom/limit
func geosPolygon(g *geos.Geos, polygon geojson.Polygon) (*geos.Geom, error) {
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

// TODO duplicate of imposm3/geom/limit
func transformPolygon(p geojson.Polygon, targetSRID int) {
	if targetSRID != 3857 {
		panic("transformation to non-4326/3856 not implemented")
	}
	for _, ls := range p {
		for i := range ls {
			ls[i].Long, ls[i].Lat = proj.WgsToMerc(ls[i].Long, ls[i].Lat)
		}
	}
}
