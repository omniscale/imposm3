package diff

import (
	"goposm/cache"
	"goposm/database"
	"goposm/diff/parser"
	"goposm/mapping"
	"log"
)

type Deleter struct {
	delDb         database.Deleter
	osmCache      *cache.OSMCache
	diffCache     *cache.DiffCache
	tmPoints      *mapping.TagMatcher
	tmLineStrings *mapping.TagMatcher
	tmPolygons    *mapping.TagMatcher
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
	}
}

func (d *Deleter) deleteRelation(id int64) {
	elem, err := d.osmCache.Relations.GetRelation(id)
	if err != nil {
		if err == cache.NotFound {
			return
		}
		// TODO
		log.Println("rel", id, err)
		return
	}
	if elem.Tags == nil {
		return
	}
	for _, m := range d.tmPolygons.Match(&elem.Tags) {
		d.delDb.Delete(m.Table.Name, elem.Id)
	}
}

func (d *Deleter) deleteWay(id int64) {
	elem, err := d.osmCache.Ways.GetWay(id)
	if err != nil {
		if err == cache.NotFound {
			return
		}
		// TODO
		log.Println("way", id, err)
		return
	}
	if elem.Tags == nil {
		return
	}
	for _, m := range d.tmPolygons.Match(&elem.Tags) {
		d.delDb.Delete(m.Table.Name, elem.Id)
	}
	for _, m := range d.tmLineStrings.Match(&elem.Tags) {
		d.delDb.Delete(m.Table.Name, elem.Id)
	}
}

func (d *Deleter) deleteNode(id int64) {
	elem, err := d.osmCache.Nodes.GetNode(id)
	if err != nil {
		if err == cache.NotFound {
			return
		}
		// TODO
		log.Println("node", id, err)
		return
	}
	if elem.Tags == nil {
		return
	}
	for _, m := range d.tmPoints.Match(&elem.Tags) {
		d.delDb.Delete(m.Table.Name, elem.Id)
	}
}

func (d *Deleter) Delete(delElem parser.DiffElem) {
	if !delElem.Del {
		panic("del=false")
	}

	if delElem.Rel != nil {
		d.deleteRelation(delElem.Rel.Id)
	} else if delElem.Way != nil {
		d.deleteWay(delElem.Way.Id)

		if delElem.Mod {
			dependers := d.diffCache.Ways.Get(delElem.Way.Id)
			for _, rel := range dependers {
				d.deleteRelation(rel)
			}
		}
	} else if delElem.Node != nil {
		d.deleteNode(delElem.Node.Id)
		if delElem.Mod {
			dependers := d.diffCache.Coords.Get(delElem.Node.Id)
			for _, way := range dependers {
				d.deleteWay(way)
				dependers := d.diffCache.Ways.Get(way)
				for _, rel := range dependers {
					d.deleteRelation(rel)
				}
			}
		}
	}
}
