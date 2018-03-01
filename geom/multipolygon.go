package geom

import (
	"errors"
	"sort"

	"github.com/gregtzar/imposm3/element"
	"github.com/gregtzar/imposm3/geom/geos"
)

type PreparedRelation struct {
	rings []*ring
	rel   *element.Relation
	srid  int
}

// PrepareRelation is the first step in building a (multi-)polygon of a Relation.
// It builds rings from all ways and returns an error if there are unclosed rings.
func PrepareRelation(rel *element.Relation, srid int, maxRingGap float64) (PreparedRelation, error) {
	rings, err := buildRings(rel, maxRingGap)
	if err != nil {
		return PreparedRelation{}, err
	}

	return PreparedRelation{rings, rel, srid}, nil
}

// Build creates the (multi)polygon Geometry of the Relation.
func (prep *PreparedRelation) Build() (Geometry, error) {
	g := geos.NewGeos()
	g.SetHandleSrid(prep.srid)
	defer g.Finish()

	geom, err := buildRelGeometry(g, prep.rel, prep.rings)
	if err != nil {
		return Geometry{}, err
	}

	wkb := g.AsEwkbHex(geom)
	if wkb == nil {
		return Geometry{}, errors.New("unable to create WKB for relation")
	}
	return Geometry{Geom: geom, Wkb: wkb}, nil
}

func destroyRings(g *geos.Geos, rings []*ring) {
	for _, r := range rings {
		if r.geom != nil {
			g.Destroy(r.geom)
			r.geom = nil
		}
	}
}

func buildRings(rel *element.Relation, maxRingGap float64) ([]*ring, error) {
	var rings []*ring
	var incompleteRings []*ring
	var completeRings []*ring
	var mergedRings []*ring
	var err error
	g := geos.NewGeos()
	defer g.Finish()

	defer func() {
		if err != nil {
			destroyRings(g, mergedRings)
			destroyRings(g, completeRings)
		}
	}()

	// create rings for all WAY members
	for _, member := range rel.Members {
		if member.Way == nil {
			continue
		}
		rings = append(rings, newRing(member.Way))
	}

	// create geometries for closed rings, collect incomplete rings
	for _, r := range rings {
		if r.isClosed() {
			r.geom, err = Polygon(g, r.nodes)
			if err != nil {
				return nil, err
			}
			completeRings = append(completeRings, r)
		} else {
			incompleteRings = append(incompleteRings, r)
		}
	}
	// merge incomplete rings
	mergedRings = mergeRings(incompleteRings)

	// create geometries for merged rings
	for _, ring := range mergedRings {
		if !ring.isClosed() && !ring.tryClose(maxRingGap) {
			continue
		}
		ring.geom, err = Polygon(g, ring.nodes)
		if err != nil {
			return nil, err
		}
		completeRings = append(completeRings, ring)
	}

	if len(completeRings) == 0 {
		err = ErrorNoRing // for defer
		return nil, err
	}

	// sort by area (large to small)
	for _, r := range completeRings {
		r.area = r.geom.Area()
	}
	sort.Sort(sortableRingsDesc(completeRings))

	return completeRings, nil
}

type sortableRingsDesc []*ring

func (r sortableRingsDesc) Len() int           { return len(r) }
func (r sortableRingsDesc) Less(i, j int) bool { return r[i].area > r[j].area }
func (r sortableRingsDesc) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }

// buildRelGeometry builds the geometry of rel by creating a multipolygon of all rings.
// rings need to be sorted by area (large to small).
func buildRelGeometry(g *geos.Geos, rel *element.Relation, rings []*ring) (*geos.Geom, error) {
	totalRings := len(rings)
	shells := map[*ring]bool{rings[0]: true}
	for i := 0; i < totalRings; i++ {
		testGeom := g.Prepare(rings[i].geom)
		if testGeom == nil {
			return nil, errors.New("Error while preparing geometry")
		}
		for j := i + 1; j < totalRings; j++ {
			if g.PreparedContains(testGeom, rings[j].geom) {
				if rings[j].containedBy != -1 {
					// j is inside a larger ring, remove that relationship
					// e.g. j is hole inside a hole (i)
					delete(rings[rings[j].containedBy].holes, rings[j])
					delete(shells, rings[j])
				}
				// remember parent
				rings[j].containedBy = i
				// add ring as hole or shell
				if ringIsHole(rings, j) {
					rings[i].holes[rings[j]] = true
					rings[i].outer = false
				} else {
					shells[rings[j]] = true
					rings[i].outer = true
				}
			}
		}
		if rings[i].containedBy == -1 {
			// add as shell if it is not a hole
			shells[rings[i]] = true
			rings[i].outer = true
		}
		g.PreparedDestroy(testGeom)
	}

	var polygons []*geos.Geom
	for shell, _ := range shells {
		var interiors []*geos.Geom
		for hole, _ := range shell.holes {
			ring := g.Clone(g.ExteriorRing(hole.geom))
			g.Destroy(hole.geom)
			if ring == nil {
				return nil, errors.New("Error while getting exterior ring.")
			}
			interiors = append(interiors, ring)
		}
		exterior := g.Clone(g.ExteriorRing(shell.geom))
		g.Destroy(shell.geom)
		if exterior == nil {
			return nil, errors.New("Error while getting exterior ring.")
		}
		polygon := g.Polygon(exterior, interiors)
		if polygon == nil {
			return nil, errors.New("Error while building polygon.")
		}
		polygons = append(polygons, polygon)
	}
	var result *geos.Geom

	if len(polygons) == 1 {
		result = polygons[0]
	} else {
		result = g.MultiPolygon(polygons)
		if result == nil {
			return nil, errors.New("Error while building multi-polygon.")
		}
	}
	var err error
	result, err = g.MakeValid(result)
	if err != nil {
		return nil, err
	}

	g.DestroyLater(result)

	outer := make(map[int64]struct{})
	for i := range rings {
		if rings[i].outer {
			for _, w := range rings[i].ways {
				outer[w.Id] = struct{}{}
			}
		}
	}
	for i := range rel.Members {
		mid := rel.Members[i].Id
		if _, ok := outer[mid]; ok {
			rel.Members[i].Role = "outer"
		} else {
			rel.Members[i].Role = "inner"
		}
	}

	return result, nil
}

// ringIsHole returns true if rings[idx] is a hole, False if it is a
// shell (also if hole in a hole, etc)
func ringIsHole(rings []*ring, idx int) bool {

	containedCounter := 0
	for {
		idx = rings[idx].containedBy
		if idx == -1 {

			break
		}
		containedCounter += 1
	}
	return containedCounter%2 == 1
}
