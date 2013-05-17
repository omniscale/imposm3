package main

import (
	"flag"
	"goposm/cache"
	"log"
)

var (
	nodeId   = flag.Int64("node", -1, "node")
	wayId    = flag.Int64("way", -1, "way")
	relId    = flag.Int64("rel", -1, "relation")
	full     = flag.Bool("full", false, "recurse into relations/ways")
	cachedir = flag.String("cachedir", "/tmp/goposm", "cache directory")
)

func printRelations(osmCache *cache.OSMCache, ids []int64, recurse bool) {
	for _, id := range ids {
		rel, err := osmCache.Relations.GetRelation(id)
		if err == cache.NotFound {
			log.Println("not found")
		} else if err != nil {
			log.Fatal(err)
		} else {
			log.Println(rel)
			if recurse {
				oldPrefix := log.Prefix()
				log.SetPrefix(oldPrefix + "        ")
				defer log.SetPrefix(oldPrefix)
				for _, m := range rel.Members {
					printWays(osmCache, []int64{m.Id}, true)
				}
			}
		}
	}
}

func printWays(osmCache *cache.OSMCache, ids []int64, recurse bool) {
	for _, id := range ids {
		way, err := osmCache.Ways.GetWay(id)
		if err == cache.NotFound {
			log.Println(id, "not found")
		} else if err != nil {
			log.Fatal(err)
		} else {
			log.Println(way)
			if recurse {
				oldPrefix := log.Prefix()
				log.SetPrefix(oldPrefix + "        ")
				defer log.SetPrefix(oldPrefix)
				printNodes(osmCache, way.Refs)
			}
		}
	}
}

func printNodes(osmCache *cache.OSMCache, ids []int64) {
	for _, id := range ids {
		node, err := osmCache.Nodes.GetNode(id)
		if err != cache.NotFound && err != nil {
			log.Fatal(err)
		}
		if node == nil {
			node, err = osmCache.Coords.GetCoord(id)
			if err == cache.NotFound {
				log.Println(id, "not found")
			} else if err != nil {
				log.Fatal(err)
			}
		}
		if node != nil {
			log.Println(node)
		}
	}
}

func main() {
	flag.Parse()
	log.SetFlags(0)

	osmCache := cache.NewOSMCache(*cachedir)
	err := osmCache.Open()
	if err != nil {
		log.Fatal(err)
	}

	if *relId != -1 {
		printRelations(osmCache, []int64{*relId}, *full)
	}

	if *wayId != -1 {
		printWays(osmCache, []int64{*wayId}, *full)
	}
	if *nodeId != -1 {
		printNodes(osmCache, []int64{*nodeId})
	}

}
