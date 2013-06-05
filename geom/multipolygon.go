package geom

import (
	"errors"
	"goposm/element"
	"goposm/geom/geos"
	"sort"
)

func BuildRelation(rel *element.Relation) error {
	rings, err := BuildRings(rel)
	if err != nil {
		return err
	}
	_, err = BuildRelGeometry(rel, rings)
	if err != nil {
		return err
	}
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

	g := geos.NewGeos()
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
		return nil, ErrorNoRing
	}
	// create geometries for merged rings
	for _, ring := range mergedRings {
		if !ring.IsClosed() {
			return nil, ErrorNoRing
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

func BuildRelGeometry(rel *element.Relation, rings []*Ring) (*geos.Geom, error) {
	g := geos.NewGeos()
	defer g.Finish()

	// sort by area (large to small)
	for _, r := range rings {
		r.area = r.geom.Area()
	}
	sort.Sort(SortableRingsDesc(rings))

	totalRings := len(rings)
	shells := map[*Ring]bool{rings[0]: true}
	for i := 0; i < totalRings; i++ {
		testGeom := g.Prepare(rings[i].geom)
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

	relTags := relationTags(rel.Tags, rings[0].ways[0].Tags)

	var polygons []*geos.Geom
	for shell, _ := range shells {
		var interiors []*geos.Geom
		for hole, _ := range shell.holes {
			hole.MarkInserted(relTags)
			ring := g.Clone(g.ExteriorRing(hole.geom))
			if ring == nil {
				return nil, errors.New("Error while getting exterior ring.")
			}
			interiors = append(interiors, ring)
		}
		shell.MarkInserted(relTags)
		exterior := g.Clone(g.ExteriorRing(shell.geom))
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
	if !g.IsValid(result) {
		buffered := g.Buffer(result, 0)
		if buffered == nil {
			return nil, errors.New("Error while fixing geom with buffer(0)")
		}
		g.Destroy(result)
		result = buffered
	}

	g.DestroyLater(result)

	insertedWays := make(map[int64]bool)
	for _, r := range rings {
		for id, _ := range r.inserted {
			insertedWays[id] = true
		}
	}

	var relMembers []element.Member
	for _, m := range rel.Members {
		if _, ok := insertedWays[m.Id]; ok {
			relMembers = append(relMembers, m)
		}
	}

	rel.Members = relMembers
	wkb := g.AsWkb(result)
	if wkb == nil {
		return nil, errors.New("unable to create WKB for relation")
	}
	rel.Geom = &element.Geometry{Geom: result, Wkb: wkb}
	rel.Tags = relTags

	return result, nil
}

func relationTags(relTags, wayTags element.Tags) element.Tags {
	result := make(element.Tags)
	for k, v := range relTags {
		if k == "name" || k == "type" {
			continue
		}
		result[k] = v
	}

	if len(result) == 0 {
		// relation does not have tags? use way tags
		for k, v := range wayTags {
			result[k] = v
		}
	} else {
		// add back name (if present)
		if name, ok := relTags["name"]; ok {
			result["name"] = name
		}
	}
	return result
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
