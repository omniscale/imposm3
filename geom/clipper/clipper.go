package clipper

import (
	"errors"
	"goposm/geom/geos"
	"goposm/geom/ogr"
	"math"
	"strings"
)

// Tile bbox into multiple sub-boxes, each of `width` size.

// >>> list(tile_bbox((-1, 1, 0.49, 1.51), 0.5)) #doctest: +NORMALIZE_WHITESPACE
// [(-1.0, 1.0, -0.5, 1.5),
//  (-1.0, 1.5, -0.5, 2.0),
//  (-0.5, 1.0, 0.0, 1.5),
//  (-0.5, 1.5, 0.0, 2.0),
//  (0.0, 1.0, 0.5, 1.5),
//  (0.0, 1.5, 0.5, 2.0)]
func tileBounds(bounds geos.Bounds, width float64) []geos.Bounds {
	var results []geos.Bounds
	minX := math.Floor(bounds.MinX/width) * width
	minY := math.Floor(bounds.MinY/width) * width
	maxX := math.Ceil(bounds.MaxX/width) * width
	maxY := math.Ceil(bounds.MaxY/width) * width

	xSteps := math.Ceil((maxX - minX) / width)
	ySteps := math.Ceil((maxY - minY) / width)

	for x := 0; x < int(xSteps); x++ {
		for y := 0; y < int(ySteps); y++ {
			results = append(results, geos.Bounds{
				minX + float64(x)*width,
				minY + float64(y)*width,
				minX + float64(x+1)*width,
				minY + float64(y+1)*width,
			})
		}
	}
	return results
}

func SplitPolygonAtGrid(g *geos.Geos, geom *geos.Geom, gridWidth, currentGridWidth float64) ([]*geos.Geom, error) {
	// >>> p = list(split_polygon_at_grid(geometry.box(-0.5, 1, 0.2, 2), 1))
	// >>> p[0].contains(geometry.box(-0.5, 1, 0, 2))
	// True
	// >>> p[0].area == geometry.box(-0.5, 1, 0, 2).area
	// True
	// >>> p[1].contains(geometry.box(0, 1, 0.2, 2))
	// True
	// >>> p[1].area == geometry.box(0, 1, 0.2, 2).area
	// True

	// if not geom.is_valid:
	//     geom = geom.buffer(0)

	var result []*geos.Geom
	geomBounds := geom.Bounds()
	if geomBounds == geos.NilBounds {
		return nil, errors.New("couldn't create bounds for geom")
	}
	for _, bounds := range tileBounds(geom.Bounds(), currentGridWidth) {
		clipGeom := g.BoundsPolygon(bounds)
		if clipGeom == nil {
			return nil, errors.New("couldn't create bounds polygon")
		}
		part := g.Intersection(geom, clipGeom)
		if part == nil {
			return nil, errors.New("couldn't create intersection")
		}
		if !g.IsEmpty(part) && strings.HasPrefix(g.Type(part), "Polygon") {
			if gridWidth >= currentGridWidth {
				result = append(result, part)
			} else {
				moreParts, err := SplitPolygonAtGrid(g, part, gridWidth, currentGridWidth/10.0)
				if err != nil {
					return nil, err
				}
				result = append(result, moreParts...)
			}
		}
	}
	return result, nil
}

type Clipper struct {
	index *geos.Index
}

func NewFromOgrSource(source string) (*Clipper, error) {
	ds, err := ogr.Open(source)
	if err != nil {
		return nil, err
	}

	g := geos.NewGeos()
	defer g.Finish()

	layer, err := ds.Layer()
	if err != nil {
		return nil, err
	}

	index := g.CreateIndex()

	for geom := range layer.Geoms() {
		parts, err := SplitPolygonAtGrid(g, geom, 10000, 10000*100)
		if err != nil {
			return nil, err
		}
		for _, part := range parts {
			g.IndexAdd(index, part)
		}
	}
	return &Clipper{index}, nil
}

func filterGeometryByType(g *geos.Geos, geom *geos.Geom, targetType string) []*geos.Geom {
	// Filter (multi)geometry for compatible `geom_type`,
	// because we can't insert points into linestring tables for example

	geomType := g.Type(geom)

	if geomType == targetType {
		// same type is fine
		return []*geos.Geom{geom}
	}
	if geomType == "Polygon" && targetType == "MultiPolygon" {
		// multipolygon mappings also support polygons
		return []*geos.Geom{geom}
	}
	if geomType == "MultiPolygon" && targetType == "Polygon" {
		// polygon mappings should also support multipolygons
		return []*geos.Geom{geom}
	}

	if g.NumGeoms(geom) >= 1 {
		// GeometryCollection or MultiLineString? return list of geometries
		var geoms []*geos.Geom
		for _, part := range g.Geoms(geom) {
			// only parts with same type
			if g.Type(part) == targetType {
				geoms = append(geoms, part)
			}
		}
		if len(geoms) != 0 {
			return geoms
		}
	}
	return []*geos.Geom{}
}

func (clipper *Clipper) clip(geom *geos.Geom) ([]*geos.Geom, error) {
	g := geos.NewGeos()
	defer g.Finish()

	hits := g.IndexQuery(clipper.index, geom)

	if len(hits) == 0 {
		return nil, nil
	}
	geomType := g.Type(geom)

	var intersections []*geos.Geom

	for _, hit := range hits {
		if g.Contains(hit, geom) {
			return []*geos.Geom{geom}, nil
		}

		if g.Intersects(hit, geom) {
			newPart := g.Intersection(hit, geom)
			newParts := filterGeometryByType(g, newPart, geomType)
			for _, p := range newParts {
				intersections = append(intersections, p)
			}
		}
	}

	return mergeGeometries(g, intersections, geomType), nil
	// if not intersections:
	//     raise EmtpyGeometryError('No intersection or empty geometry')

	// # intersections from multiple sub-polygons
	// # try to merge them back to a single geometry
	// try:
	//     if geom.type.endswith('Polygon'):
	//         union = cascaded_union(list(flatten_polygons(intersections)))
	//     elif geom.type.endswith('LineString'):
	//         linestrings = flatten_linestrings(intersections)
	//         linestrings = list(filter_invalid_linestrings(linestrings))
	//         if not linestrings:
	//             raise EmtpyGeometryError()
	//         union = linemerge(linestrings)
	//         if union.type == 'MultiLineString':
	//             union = list(union.geoms)
	//     elif geom.type == 'Point':
	//         union = intersections[0]
	//     else:
	//         log.warn('unexpexted geometry type %s', geom.type)
	//         raise EmtpyGeometryError()
	// except ValueError, ex:
	//     # likely an 'No Shapely geometry can be created from null value' error
	//     log.warn('could not create union: %s', ex)
	//     raise EmtpyGeometryError()
	// return union

}

func flattenPolygons(g *geos.Geos, geoms []*geos.Geom) []*geos.Geom {
	var result []*geos.Geom
	for _, geom := range geoms {
		if g.Type(geom) == "MultiPolygon" {
			result = append(result, g.Geoms(geom)...)
		} else {
			result = append(result, geom)
		}
	}
	return result
}

func flattenLineStrings(g *geos.Geos, geoms []*geos.Geom) []*geos.Geom {
	var result []*geos.Geom
	for _, geom := range geoms {
		if g.Type(geom) == "MultiLineString" {
			result = append(result, g.Geoms(geom)...)
		} else {
			result = append(result, geom)
		}
	}
	return result
}

func filterInvalidLineStrings(g *geos.Geos, geoms []*geos.Geom) []*geos.Geom {
	var result []*geos.Geom
	for _, geom := range geoms {
		if geom.Length() > 1e-9 {
			result = append(result, geom)
		}
	}
	return result
}

// mergeGeometries
func mergeGeometries(g *geos.Geos, geoms []*geos.Geom, geomType string) []*geos.Geom {
	// intersections from multiple sub-polygons
	// try to merge them back to a single geometry
	if strings.HasSuffix(geomType, "Polygon") {
		polygons := flattenPolygons(g, geoms)
		polygon := g.UnionPolygons(polygons)
		return []*geos.Geom{polygon}
	} else if strings.HasSuffix(geomType, "LineString") {
		linestrings := flattenLineStrings(g, geoms)
		linestrings = filterInvalidLineStrings(g, linestrings)
		if len(linestrings) == 0 {
			return nil
		}
		union := g.LineMerge(linestrings)
		return union
	} else if geomType == "Point" {
		return geoms[0:1]
	} else {
		panic("unexpected geometry type" + geomType)
	}
}
