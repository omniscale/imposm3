package diff

import (
	"imposm3/cache"
	"imposm3/database"
	"imposm3/diff/parser"
	"imposm3/element"
	"imposm3/expire"
	"imposm3/mapping"
	"imposm3/proj"
)

type Deleter struct {
	delDb            database.Deleter
	osmCache         *cache.OSMCache
	diffCache        *cache.DiffCache
	tmPoints         *mapping.TagMatcher
	tmLineStrings    *mapping.TagMatcher
	tmPolygons       *mapping.TagMatcher
	expireor         expire.Expireor
	deletedRelations map[int64]struct{}
	deletedWays      map[int64]struct{}
	deletedMembers   map[int64]struct{}
}

func NewDeleter(db database.Deleter, osmCache *cache.OSMCache, diffCache *cache.DiffCache,
	tmPoints *mapping.TagMatcher,
	tmLineStrings *mapping.TagMatcher,
	tmPolygons *mapping.TagMatcher,
) *Deleter {
	return &Deleter{
		db,
		osmCache,
		diffCache,
		tmPoints,
		tmLineStrings,
		tmPolygons,
		nil,
		make(map[int64]struct{}),
		make(map[int64]struct{}),
		make(map[int64]struct{}),
	}
}

func (d *Deleter) SetExpireor(exp expire.Expireor) {
	d.expireor = exp
}

func (d *Deleter) DeletedMemberWays() map[int64]struct{} {
	return d.deletedMembers
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
	if ok, matches := d.delDb.ProbePolygon(elem.OSMElem); ok {
		if err := d.delDb.Delete(-elem.Id, matches); err != nil {
			return err
		}
	} else {
		// handle relations with tags from members by deleting
		// from all tables
		e := element.OSMElem(elem.OSMElem)
		e.Id = -e.Id
		if err := d.delDb.DeleteElem(e); err != nil {
			return err
		}
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
		for _, m := range elem.Members {
			if m.Way == nil {
				continue
			}
			err := d.osmCache.Coords.FillWay(m.Way)
			if err != nil {
				continue
			}
			proj.NodesToMerc(m.Way.Nodes)
			expire.ExpireNodes(d.expireor, m.Way.Nodes)
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
	if ok, matches := d.delDb.ProbePolygon(elem.OSMElem); ok {
		if err := d.delDb.Delete(elem.Id, matches); err != nil {
			return err
		}
		deleted = true
	}
	if ok, matches := d.delDb.ProbeLineString(elem.OSMElem); ok {
		if err := d.delDb.Delete(elem.Id, matches); err != nil {
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
		expire.ExpireNodes(d.expireor, elem.Nodes)
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

	if ok, matches := d.delDb.ProbePoint(elem.OSMElem); ok {
		if err := d.delDb.Delete(elem.Id, matches); err != nil {
			return err
		}
		deleted = true
	}

	if deleted && d.expireor != nil {
		d.expireor.Expire(elem.Long, elem.Lat)
	}
	return nil
}

func (d *Deleter) Delete(delElem parser.DiffElem) error {
	if !delElem.Del {
		panic("del=false")
	}

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
		}
		if !delElem.Add {
			if err := d.diffCache.Coords.Delete(delElem.Node.Id); err != nil {
				return err
			}
		}
	}
	return nil
}
