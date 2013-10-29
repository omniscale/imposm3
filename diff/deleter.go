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
	expireTiles      *expire.Tiles
	deletedRelations map[int64]struct{}
	deletedWays      map[int64]struct{}
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
	}
}

func (d *Deleter) SetExpireTiles(expireTiles *expire.Tiles) {
	d.expireTiles = expireTiles
}

func (d *Deleter) deleteRelation(id int64, deleteRefs bool) {
	d.deletedRelations[id] = struct{}{}

	elem, err := d.osmCache.Relations.GetRelation(id)
	if err != nil {
		if err == cache.NotFound {
			return
		}
		// TODO
		log.Print("rel", id, err)
		return
	}
	if elem.Tags == nil {
		return
	}
	deleted := false
	if ok, matches := d.delDb.ProbePolygon(elem.OSMElem); ok {
		d.delDb.Delete(elem.Id, matches)
		deleted = true
	}

	if deleted && deleteRefs {
		for _, m := range elem.Members {
			if m.Type == element.WAY {
				d.diffCache.Ways.DeleteRef(m.Id, id)
			}
		}
	}

	d.osmCache.InsertedWays.DeleteMembers(elem.Members)
	if deleted && d.expireTiles != nil {
		for _, m := range elem.Members {
			if m.Way == nil {
				continue
			}
			err := d.osmCache.Coords.FillWay(m.Way)
			if err != nil {
				continue
			}
			proj.NodesToMerc(m.Way.Nodes)
			d.expireTiles.ExpireFromNodes(m.Way.Nodes)
		}
	}
}

func (d *Deleter) deleteWay(id int64, deleteRefs bool) {
	d.deletedWays[id] = struct{}{}

	elem, err := d.osmCache.Ways.GetWay(id)
	if err != nil {
		if err == cache.NotFound {
			return
		}
		// TODO
		log.Print("way", id, err)
		return
	}
	if elem.Tags == nil {
		return
	}
	deleted := false
	if ok, matches := d.delDb.ProbePolygon(elem.OSMElem); ok {
		d.delDb.Delete(elem.Id, matches)
		deleted = true
	}
	if ok, matches := d.delDb.ProbeLineString(elem.OSMElem); ok {
		d.delDb.Delete(elem.Id, matches)
		deleted = true
	}
	if deleted && deleteRefs {
		for _, n := range elem.Refs {
			d.diffCache.Coords.DeleteRef(n, id)
		}
	}
	if deleted && d.expireTiles != nil {
		err := d.osmCache.Coords.FillWay(elem)
		if err != nil {
			return
		}
		d.expireTiles.ExpireFromNodes(elem.Nodes)
	}
}

func (d *Deleter) deleteNode(id int64) {
	elem, err := d.osmCache.Nodes.GetNode(id)
	if err != nil {
		if err == cache.NotFound {
			return
		}
		// TODO
		log.Print("node", id, err)
		return
	}
	if elem.Tags == nil {
		return
	}
	deleted := false

	if ok, matches := d.delDb.ProbePoint(elem.OSMElem); ok {
		d.delDb.Delete(elem.Id, matches)
		deleted = true
	}

	if deleted && d.expireTiles != nil {
		d.expireTiles.ExpireFromNodes([]element.Node{*elem})
	}

}

func (d *Deleter) Delete(delElem parser.DiffElem) {
	if !delElem.Del {
		panic("del=false")
	}

	if delElem.Rel != nil {
		d.deleteRelation(delElem.Rel.Id, true)
	} else if delElem.Way != nil {
		d.deleteWay(delElem.Way.Id, true)

		if delElem.Mod {
			dependers := d.diffCache.Ways.Get(delElem.Way.Id)
			for _, rel := range dependers {
				if _, ok := d.deletedRelations[rel]; ok {
					continue
				}
				d.deleteRelation(rel, false)
			}
		}
	} else if delElem.Node != nil {
		d.deleteNode(delElem.Node.Id)
		if delElem.Mod {
			dependers := d.diffCache.Coords.Get(delElem.Node.Id)
			for _, way := range dependers {
				if _, ok := d.deletedWays[way]; ok {
					continue
				}
				d.deleteWay(way, false)
				dependers := d.diffCache.Ways.Get(way)
				for _, rel := range dependers {
					if _, ok := d.deletedRelations[rel]; ok {
						continue
					}
					d.deleteRelation(rel, false)
				}
			}
		}
		if !delElem.Add {
			d.diffCache.Coords.Delete(delElem.Node.Id)
		}
	}
}
