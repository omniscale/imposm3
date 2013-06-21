package main

import (
	"flag"
	"fmt"
	"goposm/cache"
	"goposm/database"
	_ "goposm/database/postgis"
	"goposm/diff"
	"goposm/diff/parser"
	"goposm/element"
	"goposm/geom/clipper"
	"goposm/mapping"
	"goposm/stats"
	"goposm/writer"
	"io"
	"log"
)

var (
	connection = flag.String("connection", "", "connection parameters")
)

func main() {
	flag.Parse()
	elems, errc := parser.Parse(flag.Arg(0))

	osmCache := cache.NewOSMCache("/tmp/goposm")
	err := osmCache.Open()
	if err != nil {
		log.Fatal(err)
	}

	diffCache := cache.NewDiffCache("/tmp/goposm")
	err = diffCache.Open()
	if err != nil {
		log.Fatal(err)
	}

	tagmapping, err := mapping.NewMapping("../../mapping.json")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(*connection)
	connType := database.ConnectionType(*connection)
	conf := database.Config{
		Type:             connType,
		ConnectionParams: *connection,
		Srid:             3857,
	}
	db, err := database.Open(conf, tagmapping)
	if err != nil {
		log.Fatal(err)
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

	relTagFilter := tagmapping.RelationTagFilter()
	wayTagFilter := tagmapping.WayTagFilter()
	nodeTagFilter := tagmapping.NodeTagFilter()

	pointsTagMatcher := tagmapping.PointMatcher()
	lineStringsTagMatcher := tagmapping.LineStringMatcher()
	polygonsTagMatcher := tagmapping.PolygonMatcher()

	relations := make(chan *element.Relation)
	ways := make(chan *element.Way)
	nodes := make(chan *element.Node)

	srid := 3857 // TODO

	relWriter := writer.NewRelationWriter(osmCache, diffCache, relations,
		db, polygonsTagMatcher, progress, srid)
	relWriter.SetClipper(geometryClipper)
	relWriter.Start()

	wayWriter := writer.NewWayWriter(osmCache, diffCache, ways, db,
		lineStringsTagMatcher, polygonsTagMatcher, progress, srid)
	wayWriter.SetClipper(geometryClipper)
	wayWriter.Start()

	nodeWriter := writer.NewNodeWriter(osmCache, nodes, db,
		pointsTagMatcher, progress, srid)
	nodeWriter.SetClipper(geometryClipper)
	nodeWriter.Start()

	nodeIds := make(map[int64]bool)
	wayIds := make(map[int64]bool)
	relIds := make(map[int64]bool)

For:
	for {
		select {
		case elem := <-elems:
			if elem.Del {
				deleter.Delete(elem)
				if !elem.Add {
					// TODO delete from osmCache
				}
			}
			if elem.Add {
				if elem.Rel != nil {
					relTagFilter.Filter(&elem.Rel.Tags)
					osmCache.Relations.PutRelation(elem.Rel)
					relIds[elem.Rel.Id] = true
				} else if elem.Way != nil {
					wayTagFilter.Filter(&elem.Way.Tags)
					osmCache.Ways.PutWay(elem.Way)
					wayIds[elem.Way.Id] = true
				} else if elem.Node != nil {
					nodeTagFilter.Filter(&elem.Node.Tags)
					osmCache.Nodes.PutNode(elem.Node)
					osmCache.Coords.PutCoords([]element.Node{*elem.Node})
					nodeIds[elem.Node.Id] = true
				}
			}
		case err := <-errc:
			if err == io.EOF {
				fmt.Println("done")
			} else {
				fmt.Println(err)
			}
			break For
		}
	}

	for nodeId, _ := range nodeIds {
		node, err := osmCache.Nodes.GetNode(nodeId)
		if err == nil {
			nodes <- node
			// missing nodes can still be Coords
		}
		dependers := diffCache.Coords.Get(nodeId)
		for _, way := range dependers {
			wayIds[way] = true
		}
	}

	for wayId, _ := range wayIds {
		way, err := osmCache.Ways.GetWay(wayId)
		if err != nil {
			log.Println(wayId, err)
			continue
		}
		ways <- way
		dependers := diffCache.Ways.Get(wayId)
		for _, rel := range dependers {
			relIds[rel] = true
		}
	}

	for relId, _ := range relIds {
		rel, err := osmCache.Relations.GetRelation(relId)
		if err != nil {
			log.Println(err)
			continue
		}
		relations <- rel
	}

	close(relations)
	close(ways)
	close(nodes)

	nodeWriter.Close()
	relWriter.Close()
	wayWriter.Close()

	progress.Stop()
	osmCache.Coords.Flush()
	osmCache.Close()
}
