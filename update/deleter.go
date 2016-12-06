package update

import (
	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/database"
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/expire"
	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/parser/diff"
)

type Deleter struct {
	delDb            database.Deleter
	osmCache         *cache.OSMCache
	diffCache        *cache.DiffCache
	tmPoints         mapping.NodeMatcher
	tmLineStrings    mapping.WayMatcher
	tmPolygons       mapping.RelWayMatcher
	expireor         expire.Expireor
	singleIdSpace    bool
	deletedRelations map[int64]struct{}
	deletedWays      map[int64]struct{}
	deletedMembers   map[int64]struct{}
}

func NewDeleter(db database.Deleter, osmCache *cache.OSMCache, diffCache *cache.DiffCache,
	singleIdSpace bool,
	tmPoints mapping.NodeMatcher,
	tmLineStrings mapping.WayMatcher,
	tmPolygons mapping.RelWayMatcher,
) *Deleter {
	return &Deleter{
		delDb:            db,
		osmCache:         osmCache,
		diffCache:        diffCache,
		tmPoints:         tmPoints,
		tmLineStrings:    tmLineStrings,
		tmPolygons:       tmPolygons,
		singleIdSpace:    singleIdSpace,
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

func (d *Deleter) nodeId(id int64) int64 {
	return id
}

func (d *Deleter) WayId(id int64) int64 {
	if !d.singleIdSpace {
		return id
	}
	return -id
}

func (d *Deleter) RelId(id int64) int64 {
	if !d.singleIdSpace {
		return -id
	}
	return element.RelIdOffset - id
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
	// delete from all tables to handle relations with tags from members
	// and relation_members
	e := element.OSMElem(elem.OSMElem)
	e.Id = d.RelId(e.Id)
	if err := d.delDb.DeleteElem(e); err != nil {
		return err
	}

	if deleteRefs {
		for _, m := range elem.Members {
			if m.Type == element.WAY {
				if err := d.diffCache.Ways.DeleteRef(m.Id, id); err != nil {
					return err
				}
			}
		}
	}

	if deleteMembers {
		// delete members from db and force reinsert of members
		// use case: relation is deleted and member now stands for its own
		for _, member := range elem.Members {
			if member.Type == element.WAY {
				d.deletedMembers[member.Id] = struct{}{}
				if _, ok := d.deletedWays[member.Id]; ok {
					continue
				}
				for _, r := range d.diffCache.Ways.Get(member.Id) {
					if err := d.deleteRelation(r, false, false); err != nil {
						return err
					}
				}
				if err := d.deleteWay(member.Id, false); err != nil {
					return err
				}
			}
		}
	}

	if err := d.osmCache.InsertedWays.DeleteMembers(elem.Members); err != nil {
		return err
	}
	if d.expireor != nil {
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
			expire.ExpireProjectedNodes(d.expireor, m.Way.Nodes, 4326, true)
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
		if err := d.delDb.Delete(d.WayId(elem.Id), matches); err != nil {
			return err
		}
		deleted = true
		deletedPolygon = true
	}
	if matches := d.tmLineStrings.MatchWay(elem); len(matches) > 0 {
		if err := d.delDb.Delete(d.WayId(elem.Id), matches); err != nil {
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
		if err := d.delDb.Delete(d.nodeId(elem.Id), matches); err != nil {
			return err
		}
		deleted = true
	}

	if deleted && d.expireor != nil {
		d.expireor.Expire(elem.Long, elem.Lat)
	}
	return nil
}

func (d *Deleter) Delete(delElem diff.Element) error {
	if delElem.Rel != nil {
		if err := d.deleteRelation(delElem.Rel.Id, true, true); err != nil {
			return err
		}
	} else if delElem.Way != nil {
		if err := d.deleteWay(delElem.Way.Id, true); err != nil {
			return err
		}

		if delElem.Mod {
			dependers := d.diffCache.Ways.Get(delElem.Way.Id)
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
		if err := d.deleteNode(delElem.Node.Id); err != nil {
			return err
		}
		if delElem.Mod {
			dependers := d.diffCache.Coords.Get(delElem.Node.Id)
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
			dependers = d.diffCache.CoordsRel.Get(delElem.Node.Id)
			for _, rel := range dependers {
				if _, ok := d.deletedRelations[rel]; ok {
					continue
				}
				if err := d.deleteRelation(rel, false, false); err != nil {
					return err
				}
			}
		}
		if delElem.Del {
			if err := d.diffCache.Coords.Delete(delElem.Node.Id); err != nil {
				return err
			}
		}
	}
	return nil
}
