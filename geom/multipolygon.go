package geom

import (
	"errors"
	"fmt"
	"goposm/element"
	"goposm/geom/geos"
	"sort"
)

func BuildRelation(rel *element.Relation) error {
	rings, err := BuildRings(rel)
	if err != nil {
		return err
	}
	geom, err := BuildGeometry(rings)
	if err != nil {
		return err
	}
	rel.Geom = &element.Geometry{Geom: geom}
	return nil
}

func BuildRings(rel *element.Relation) ([]*Ring, error) {
	var rings []*Ring
	var incompleteRings []*Ring
	var completeRings []*Ring
	var err error

	// create rings for all WAY members
	for _, member := range rel.Members {
		if member.Way == nil {
			continue
		}
		rings = append(rings, NewRing(member.Way))
	}

	g := geos.NewGEOS()
	defer g.Finish()

	// create geometries for closed rings, collect incomplete rings
	for _, r := range rings {
		if r.IsClosed() {
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
	mergedRings := mergeRings(incompleteRings)
	if len(completeRings)+len(mergedRings) == 0 {
		return nil, errors.New(
			fmt.Sprintf("linestring from relation %d has no rings", rel.Id),
		)
	}
	// create geometries for merged rings
	for _, ring := range mergedRings {
		if !ring.IsClosed() {
			return nil, errors.New(
				fmt.Sprintf("linestrings from relation %d do not form a ring", rel.Id),
			)
		}
		ring.geom, err = Polygon(g, ring.nodes)
		if err != nil {
			return nil, err
		}
	}

	completeRings = append(completeRings, mergedRings...)
	return completeRings, nil
}

type SortableRingsDesc []*Ring

func (r SortableRingsDesc) Len() int           { return len(r) }
func (r SortableRingsDesc) Less(i, j int) bool { return r[i].area > r[j].area }
func (r SortableRingsDesc) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }

func BuildGeometry(rings []*Ring) (*geos.Geom, error) {
	g := geos.NewGEOS()
	defer g.Finish()

	// sort by area (large to small)
	for _, r := range rings {
		r.area = r.geom.Area()
	}
	sort.Sort(SortableRingsDesc(rings))

	totalRings := len(rings)
	shells := map[*Ring]bool{rings[0]: true}
	for i := 0; i < totalRings; i++ {
		testGeom := rings[i].geom //TODO prepared
		for j := i + 1; j < totalRings; j++ {
			if g.Contains(testGeom, rings[j].geom) {
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
				} else {
					shells[rings[j]] = true
				}
			}
		}
		if rings[i].containedBy == -1 {
			// add as shell if it is not a hole
			shells[rings[i]] = true
		}
	}

	var polygons []*geos.Geom
	for shell, _ := range shells {
		var interiors []*geos.Geom
		for hole, _ := range shell.holes {
			ring := g.ExteriorRing(hole.geom)
			if ring == nil {
				return nil, errors.New("Error while getting exterior ring.")
			}
			interiors = append(interiors, ring)
		}
		exterior := g.ExteriorRing(shell.geom)
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

	return result, nil
}

// ringIsHole returns true if rings[idx] is a hole, False if it is a
// shell (also if hole in a hole, etc)
func ringIsHole(rings []*Ring, idx int) bool {

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
