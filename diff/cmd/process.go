package main

import (
	"flag"
	"fmt"
	"goposm/cache"
	"goposm/config"
	"goposm/database"
	_ "goposm/database/postgis"
	"goposm/diff"
	"goposm/diff/parser"
	"goposm/element"
	"goposm/expire"
	"goposm/geom/clipper"
	"goposm/logging"
	"goposm/mapping"
	"goposm/stats"
	"goposm/writer"
	"io"
	"os"
)

var log = logging.NewLogger("")

func main() {
	flag.Parse()
	conf, errs := config.Parse()
	if len(errs) > 0 {
		log.Warn("errors in config/options:")
		for _, err := range errs {
			log.Warnf("\t%s", err)
		}
		logging.Shutdown()
		os.Exit(1)
	}
	for _, oscFile := range flag.Args() {
		update(oscFile, conf)
	}
}

func update(oscFile string, conf *config.Config) {
	defer log.StopStep(log.StartStep(fmt.Sprintf("Processing %s", oscFile)))

	elems, errc := parser.Parse(oscFile)

	osmCache := cache.NewOSMCache(conf.CacheDir)
	err := osmCache.Open()
	if err != nil {
		log.Fatal("osm cache: ", err)
	}

	diffCache := cache.NewDiffCache(conf.CacheDir)
	err = diffCache.Open()
	if err != nil {
		log.Fatal("diff cache: ", err)
	}

	tagmapping, err := mapping.NewMapping(conf.MappingFile)
	if err != nil {
		log.Fatal(err)
	}

	connType := database.ConnectionType(conf.Connection)
	dbConf := database.Config{
		Type:             connType,
		ConnectionParams: conf.Connection,
		Srid:             conf.Srid,
	}
	db, err := database.Open(dbConf, tagmapping)
	if err != nil {
		log.Fatal("database open: ", err)
	}

	err = db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	delDb, ok := db.(database.Deleter)
	if !ok {
		log.Fatal("database not deletable")
	}
	deleter := diff.NewDeleter(
		delDb,
		osmCache,
		diffCache,
		tagmapping.PointMatcher(),
		tagmapping.LineStringMatcher(),
		tagmapping.PolygonMatcher(),
	)

	progress := stats.StatsReporter()

	var geometryClipper *clipper.Clipper

	expiredTiles := expire.NewTiles(14)

	relTagFilter := tagmapping.RelationTagFilter()
	wayTagFilter := tagmapping.WayTagFilter()
	nodeTagFilter := tagmapping.NodeTagFilter()

	pointsTagMatcher := tagmapping.PointMatcher()
	lineStringsTagMatcher := tagmapping.LineStringMatcher()
	polygonsTagMatcher := tagmapping.PolygonMatcher()

	relations := make(chan *element.Relation)
	ways := make(chan *element.Way)
	nodes := make(chan *element.Node)

	relWriter := writer.NewRelationWriter(osmCache, diffCache, relations,
		db, polygonsTagMatcher, progress, conf.Srid)
	relWriter.SetClipper(geometryClipper)
	relWriter.SetExpireTiles(expiredTiles)
	relWriter.Start()

	wayWriter := writer.NewWayWriter(osmCache, diffCache, ways, db,
		lineStringsTagMatcher, polygonsTagMatcher, progress, conf.Srid)
	wayWriter.SetClipper(geometryClipper)
	wayWriter.SetExpireTiles(expiredTiles)
	wayWriter.Start()

	nodeWriter := writer.NewNodeWriter(osmCache, nodes, db,
		pointsTagMatcher, progress, conf.Srid)
	nodeWriter.SetClipper(geometryClipper)
	nodeWriter.Start()

	nodeIds := make(map[int64]bool)
	wayIds := make(map[int64]bool)
	relIds := make(map[int64]bool)

	step := log.StartStep("Parsing changes, updating cache and removing elements")

	progress.Start()
For:
	for {
		select {
		case elem := <-elems:
			if elem.Rel != nil {
				relTagFilter.Filter(&elem.Rel.Tags)
				progress.AddRelations(1)
			} else if elem.Way != nil {
				wayTagFilter.Filter(&elem.Way.Tags)
				progress.AddWays(1)
			} else if elem.Node != nil {
				nodeTagFilter.Filter(&elem.Node.Tags)
				if len(elem.Node.Tags) > 0 {
					progress.AddNodes(1)
				}
				progress.AddCoords(1)
			}
			if elem.Del {
				deleter.Delete(elem)
				if !elem.Add {
					if elem.Rel != nil {
						if err := osmCache.Relations.DeleteRelation(elem.Rel.Id); err != nil {
							log.Fatal(err)
						}
					} else if elem.Way != nil {
						if err := osmCache.Ways.DeleteWay(elem.Way.Id); err != nil {
							log.Fatal(err)
						}
						diffCache.Ways.Delete(elem.Way.Id)
					} else if elem.Node != nil {
						if err := osmCache.Nodes.DeleteNode(elem.Node.Id); err != nil {
							log.Fatal(err)
						}
						if err := osmCache.Coords.DeleteCoord(elem.Node.Id); err != nil {
							log.Fatal(err)
						}
						diffCache.Coords.Delete(elem.Node.Id)
					}
				}
			}
			if elem.Add {
				if elem.Rel != nil {
					// TODO: check for existence of first way member
					osmCache.Relations.PutRelation(elem.Rel)
					relIds[elem.Rel.Id] = true
				} else if elem.Way != nil {
					// TODO: check for existence of first ref
					osmCache.Ways.PutWay(elem.Way)
					wayIds[elem.Way.Id] = true
				} else if elem.Node != nil {
					// TODO: check for intersection with import BBOX/poly
					osmCache.Nodes.PutNode(elem.Node)
					osmCache.Coords.PutCoords([]element.Node{*elem.Node})
					nodeIds[elem.Node.Id] = true
				}
			}
		case err := <-errc:
			if err != io.EOF {
				log.Fatal(err)
			}
			break For
		}
	}
	progress.Stop()
	log.StopStep(step)
	step = log.StartStep("Writing added/modified elements")

	progress.Start()

	for nodeId, _ := range nodeIds {
		node, err := osmCache.Nodes.GetNode(nodeId)
		if err != nil {
			if err != cache.NotFound {
				log.Print(node, err)
			}
			// missing nodes can still be Coords
			// no `continue` here
		}
		if node != nil {
			// insert new node
			nodes <- node
		}
		dependers := diffCache.Coords.Get(nodeId)
		// mark depending ways for (re)insert
		for _, way := range dependers {
			wayIds[way] = true
		}
	}

	for wayId, _ := range wayIds {
		way, err := osmCache.Ways.GetWay(wayId)
		if err != nil {
			if err != cache.NotFound {
				log.Print(way, err)
			}
			continue
		}
		// insert new way
		ways <- way
		dependers := diffCache.Ways.Get(wayId)
		// mark depending relations for (re)insert
		for _, rel := range dependers {
			relIds[rel] = true
		}
	}

	for relId, _ := range relIds {
		rel, err := osmCache.Relations.GetRelation(relId)
		if err != nil {
			if err != cache.NotFound {
				log.Print(rel, err)
			}
			continue
		}
		// insert new relation
		relations <- rel
	}

	close(relations)
	close(ways)
	close(nodes)

	nodeWriter.Close()
	relWriter.Close()
	wayWriter.Close()

	err = db.End()
	if err != nil {
		log.Fatal(err)
	}
	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}

	osmCache.Close()
	diffCache.Close()
	log.StopStep(step)

	step = log.StartStep("Updating expired tiles db")
	expire.WriteTileExpireDb(
		expiredTiles.SortedTiles(),
		"/tmp/expire_tiles.db",
	)
	log.StopStep(step)
	progress.Stop()
}
