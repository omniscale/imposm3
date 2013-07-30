package limit

import (
	"errors"
	"goposm/geom/geos"
	"goposm/geom/ogr"
	"goposm/logging"
	"math"
	"strings"
	"sync"
)

var log = logging.NewLogger("limiter")

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
		if !g.IsEmpty(part) && strings.HasSuffix(g.Type(part), "Polygon") {
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

type Limiter struct {
	index          *geos.Index
	bufferedPrep   *geos.PreparedGeom
	bufferedPrepMu *sync.Mutex
	bufferedBbox   geos.Bounds
}

func NewFromOgrSource(source string) (*Limiter, error) {
	return NewFromOgrSourceWithBuffered(source, 0.0)
}

func NewFromOgrSourceWithBuffered(source string, buffer float64) (*Limiter, error) {
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

	var polygons []*geos.Geom

	withBuffer := false
	if buffer != 0.0 {
		withBuffer = true
	}

	for geom := range layer.Geoms() {
		if withBuffer {
			simplified := g.SimplifyPreserveTopology(geom, 1000)
			if simplified == nil {
				return nil, errors.New("couldn't simplify limitto")
			}
			buffered := g.Buffer(simplified, buffer)
			g.Destroy(simplified)
			if buffered == nil {
				return nil, errors.New("couldn't buffer limitto")
			}
			g.DestroyLater(buffered)
			polygons = append(polygons, buffered)
		}
		parts, err := SplitPolygonAtGrid(g, geom, 20000, 20000*100)
		if err != nil {
			return nil, err
		}
		for _, part := range parts {
			g.IndexAdd(index, part)
		}
	}

	var bbox geos.Bounds
	var prep *geos.PreparedGeom
	if len(polygons) > 0 {
		union := g.UnionPolygons(polygons)
		if union == nil {
			return nil, errors.New("unable to union limitto polygons")
		}
		simplified := g.SimplifyPreserveTopology(union, 5000)
		if simplified == nil {
			return nil, errors.New("unable to simplify limitto polygons")
		}
		g.Destroy(union)
		bbox = simplified.Bounds()
		prep = g.Prepare(simplified)
		// keep simplified around for prepared geometry
		if prep == nil {
			return nil, errors.New("unable to prepare limitto polygons")
		}
	}
	return &Limiter{index, prep, &sync.Mutex{}, bbox}, nil
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
				geoms = append(geoms, g.Clone(part))
			}
		}
		g.Destroy(geom)
		if len(geoms) != 0 {
			return geoms
		}
		return []*geos.Geom{}
	}
	g.Destroy(geom)
	return []*geos.Geom{}
}

func (l *Limiter) Clip(geom *geos.Geom) ([]*geos.Geom, error) {
	g := geos.NewGeos()
	defer g.Finish()

	hits := g.IndexQuery(l.index, geom)

	if len(hits) == 0 {
		return nil, nil
	}
	geomType := g.Type(geom)

	var intersections []*geos.Geom

	for _, hit := range hits {
		hit.Lock()
		if g.PreparedContains(hit.Prepared, geom) {
			hit.Unlock()
			return []*geos.Geom{geom}, nil
		}

		if g.PreparedIntersects(hit.Prepared, geom) {
			hit.Unlock()
			newPart := g.Intersection(hit.Geom, geom)
			if newPart == nil {
				continue
			}
			newParts := filterGeometryByType(g, newPart, geomType)
			for _, p := range newParts {
				intersections = append(intersections, p)
			}
		} else {
			hit.Unlock()
		}
	}
	return mergeGeometries(g, intersections, geomType), nil
}

func (c *Limiter) IntersectsBuffer(g *geos.Geos, x, y float64) bool {
	if c.bufferedPrep == nil {
		return true
	}
	if x < c.bufferedBbox.MinX ||
		y < c.bufferedBbox.MinY ||
		x > c.bufferedBbox.MaxX ||
		y > c.bufferedBbox.MaxY {
		return false
	}
	p := g.Point(x, y)
	if p == nil {
		return false
	}
	defer g.Destroy(p)

	c.bufferedPrepMu.Lock()
	defer c.bufferedPrepMu.Unlock()
	return g.PreparedIntersects(c.bufferedPrep, p)
}

func flattenPolygons(g *geos.Geos, geoms []*geos.Geom) []*geos.Geom {
	var result []*geos.Geom
	for _, geom := range geoms {
		if g.Type(geom) == "MultiPolygon" {
			for _, part := range g.Geoms(geom) {
				result = append(result, g.Clone(part))
			}
			g.Destroy(geom)
		} else if g.Type(geom) == "Polygon" {
			result = append(result, geom)
		} else {
			log.Printf("unexpected geometry type in flattenPolygons")
			g.Destroy(geom)
		}
	}
	return result
}

func flattenLineStrings(g *geos.Geos, geoms []*geos.Geom) []*geos.Geom {
	var result []*geos.Geom
	for _, geom := range geoms {
		if g.Type(geom) == "MultiLineString" {
			for _, part := range g.Geoms(geom) {
				result = append(result, g.Clone(part))
			}
			g.Destroy(geom)
		} else if g.Type(geom) == "LineString" {
			result = append(result, geom)
		} else {
			log.Printf("unexpected geometry type in flattenPolygons")
			g.Destroy(geom)
		}
	}
	return result
}

func filterInvalidLineStrings(g *geos.Geos, geoms []*geos.Geom) []*geos.Geom {
	var result []*geos.Geom
	for _, geom := range geoms {
		if geom.Length() > 1e-9 {
			result = append(result, geom)
		} else {
			g.Destroy(geom)
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
		if polygon == nil {
			return nil
		}
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
		if len(geoms) >= 1 {
			return geoms[0:1]
		}
		return nil
	} else {
		panic("unexpected geometry type" + geomType)
	}
}
