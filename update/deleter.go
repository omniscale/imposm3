package update

import (
	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/database"
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/expire"
	"github.com/omniscale/imposm3/mapping"
)

type Deleter struct {
	delDb            database.Deleter
	osmCache         *cache.OSMCache
	diffCache        *cache.DiffCache
	tmPoints         mapping.NodeMatcher
	tmLineStrings    mapping.WayMatcher
	tmPolygons       mapping.RelWayMatcher
	tmRelation       mapping.RelationMatcher
	tmRelationMember mapping.RelationMatcher
	expireor         expire.Expireor
	singleIDSpace    bool
	deletedRelations map[int64]struct{}
	deletedWays      map[int64]struct{}
	deletedMembers   map[int64]struct{}
}

func NewDeleter(db database.Deleter, osmCache *cache.OSMCache, diffCache *cache.DiffCache,
	singleIDSpace bool,
	tmPoints mapping.NodeMatcher,
	tmLineStrings mapping.WayMatcher,
	tmPolygons mapping.RelWayMatcher,
	tmRelation mapping.RelationMatcher,
	tmRelationMember mapping.RelationMatcher,
) *Deleter {
	return &Deleter{
		delDb:            db,
		osmCache:         osmCache,
		diffCache:        diffCache,
		tmPoints:         tmPoints,
		tmLineStrings:    tmLineStrings,
		tmPolygons:       tmPolygons,
		tmRelation:       tmRelation,
		tmRelationMember: tmRelationMember,
		singleIDSpace:    singleIDSpace,
		deletedRelations: make(map[int64]struct{}),
		deletedWays:      make(map[int64]struct{}),
		deletedMembers:   make(map[int64]struct{}),
	}
}

func (d *Deleter) SetExpireor(exp expire.Expireor) {
	d.expireor = exp
}

func (d *Deleter) DeletedMemberWays() map[int64]struct{} {
	return d.deletedMembers
}

func (d *Deleter) nodeID(id int64) int64 {
	return id
}

func (d *Deleter) WayID(id int64) int64 {
	if !d.singleIDSpace {
		return id
	}
	return -id
}

func (d *Deleter) RelID(id int64) int64 {
	if !d.singleIDSpace {
		return -id
	}
	return element.RelIDOffset - id
}

func (d *Deleter) deleteRelation(id int64, deleteRefs bool, deleteMembers bool) error {
	d.deletedRelations[id] = struct{}{}

	elem, err := d.osmCache.Relations.GetRelation(id)
	if err != nil {
		if err == cache.NotFound {
			return nil
		}
		return err
	}
	if elem.Tags == nil {
		return nil
	}

	deleted := false
	deletedPolygon := false
	if matches := d.tmPolygons.MatchRelation(elem); len(matches) > 0 {
		if err := d.delDb.Delete(d.RelID(elem.ID), matches); err != nil {
			return err
		}
		deleted = true
		deletedPolygon = true
	}
	if matches := d.tmRelation.MatchRelation(elem); len(matches) > 0 {
		if err := d.delDb.Delete(d.RelID(elem.ID), matches); err != nil {
			return err
		}
		deleted = true
	}
	if matches := d.tmRelationMember.MatchRelation(elem); len(matches) > 0 {
		if err := d.delDb.Delete(d.RelID(elem.ID), matches); err != nil {
			return err
		}
		deleted = true
	}

	if deleteRefs {
		for _, m := range elem.Members {
			if m.Type == osm.WayMember {
				if err := d.diffCache.Ways.DeleteRef(m.ID, id); err != nil {
					return err
				}
			} else if m.Type == osm.NodeMember {
				if err := d.diffCache.CoordsRel.DeleteRef(m.ID, id); err != nil {
					return err
				}
			}
		}
	}

	if deleted && d.expireor != nil {
		if err := d.osmCache.Ways.FillMembers(elem.Members); err != nil {
			return err
		}
		for _, m := range elem.Members {
			if m.Way == nil {
				continue
			}
			err := d.osmCache.Coords.FillWay(m.Way)
			if err != nil {
				continue
			}
			expire.ExpireProjectedNodes(d.expireor, m.Way.Nodes, 4326, deletedPolygon)
		}
	}
	return nil
}

func (d *Deleter) deleteWay(id int64, deleteRefs bool) error {
	d.deletedWays[id] = struct{}{}

	elem, err := d.osmCache.Ways.GetWay(id)
	if err != nil {
		if err == cache.NotFound {
			return nil
		}
		return err
	}
	if elem.Tags == nil {
		return nil
	}
	deleted := false
	deletedPolygon := false
	if matches := d.tmPolygons.MatchWay(elem); len(matches) > 0 {
		if err := d.delDb.Delete(d.WayID(elem.ID), matches); err != nil {
			return err
		}
		deleted = true
		deletedPolygon = true
	}
	if matches := d.tmLineStrings.MatchWay(elem); len(matches) > 0 {
		if err := d.delDb.Delete(d.WayID(elem.ID), matches); err != nil {
			return err
		}
		deleted = true
	}
	if deleted && deleteRefs {
		for _, n := range elem.Refs {
			if err := d.diffCache.Coords.DeleteRef(n, id); err != nil {
				return err
			}
		}
	}
	if deleted && d.expireor != nil {
		err := d.osmCache.Coords.FillWay(elem)
		if err != nil {
			return err
		}
		expire.ExpireProjectedNodes(d.expireor, elem.Nodes, 4326, deletedPolygon)
	}
	return nil
}

func (d *Deleter) deleteNode(id int64) error {
	elem, err := d.osmCache.Nodes.GetNode(id)
	if err != nil {
		if err == cache.NotFound {
			return nil
		}
		return err
	}
	if elem.Tags == nil {
		return nil
	}
	deleted := false

	if matches := d.tmPoints.MatchNode(elem); len(matches) > 0 {
		if err := d.delDb.Delete(d.nodeID(elem.ID), matches); err != nil {
			return err
		}
		deleted = true
	}

	if deleted && d.expireor != nil {
		d.expireor.Expire(elem.Long, elem.Lat)
	}
	return nil
}

// Delete deletes the provided element from all matching tables. Depending
// elements are also removed (e.g. all ways and relations that are affected by
// a node).
func (d *Deleter) Delete(delElem osm.Diff) error {
	if delElem.Rel != nil {
		if err := d.deleteRelation(delElem.Rel.ID, true, true); err != nil {
			return err
		}
	} else if delElem.Way != nil {
		if err := d.deleteWay(delElem.Way.ID, true); err != nil {
			return err
		}

		if delElem.Modify || delElem.Create {
			// Delete depending elements even if the element is new.
			// Overlapping initial and diff imports can result in new elements
			// that are already imported.
			dependers := d.diffCache.Ways.Get(delElem.Way.ID)
			for _, rel := range dependers {
				if _, ok := d.deletedRelations[rel]; ok {
					continue
				}
				if err := d.deleteRelation(rel, false, false); err != nil {
					return err
				}
			}
		}
	} else if delElem.Node != nil {
		if err := d.deleteNode(delElem.Node.ID); err != nil {
			return err
		}
		if delElem.Modify || delElem.Create {
			// Delete depending elements even if the element is new.
			// Overlapping initial and diff imports can result in new elements
			// that are already imported.
			dependers := d.diffCache.Coords.Get(delElem.Node.ID)
			for _, way := range dependers {
				if _, ok := d.deletedWays[way]; ok {
					continue
				}
				if err := d.deleteWay(way, false); err != nil {
					return err
				}
				dependers := d.diffCache.Ways.Get(way)
				if len(dependers) >= 1 {
					// mark member ways from deleted relations for re-insert
					d.deletedMembers[way] = struct{}{}
				}
				for _, rel := range dependers {
					if _, ok := d.deletedRelations[rel]; ok {
						continue
					}
					if err := d.deleteRelation(rel, false, false); err != nil {
						return err
					}
				}
			}
			dependers = d.diffCache.CoordsRel.Get(delElem.Node.ID)
			for _, rel := range dependers {
				if _, ok := d.deletedRelations[rel]; ok {
					continue
				}
				if err := d.deleteRelation(rel, false, false); err != nil {
					return err
				}
			}
		}
		if delElem.Delete {
			if err := d.diffCache.Coords.Delete(delElem.Node.ID); err != nil {
				return err
			}
		}
	}
	return nil
}
