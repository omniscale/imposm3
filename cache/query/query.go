package query

import (
	"flag"
	"fmt"
	"log"
	"os"

	"goposm/cache"
)

var flags = flag.NewFlagSet("query-cache", flag.ExitOnError)

var (
	nodeId   = flags.Int64("node", -1, "node")
	wayId    = flags.Int64("way", -1, "way")
	relId    = flags.Int64("rel", -1, "relation")
	full     = flags.Bool("full", false, "recurse into relations/ways")
	deps     = flags.Bool("deps", false, "show dependent ways/relations")
	cachedir = flags.String("cachedir", "/tmp/goposm", "cache directory")
)

func printRelations(osmCache *cache.OSMCache, ids []int64, recurse bool) {
	for _, id := range ids {
		rel, err := osmCache.Relations.GetRelation(id)
		if err == cache.NotFound {
			log.Println("rel:", id, "not found")
		} else if err != nil {
			log.Fatal(err)
		} else {
			log.Println("rel:", rel)
			if recurse {
				oldPrefix := log.Prefix()
				log.SetPrefix(oldPrefix + "        ")
				for _, m := range rel.Members {
					printWays(osmCache, nil, []int64{m.Id}, true, false)
				}
				log.SetPrefix(oldPrefix)
			}
		}
	}
}

func printWays(osmCache *cache.OSMCache, diffCache *cache.DiffCache, ids []int64, recurse, deps bool) {
	for _, id := range ids {
		way, err := osmCache.Ways.GetWay(id)
		if err == cache.NotFound {
			log.Println("way:", id, "not found")
		} else if err != nil {
			log.Fatal(err)
		} else {
			log.Println("way:", way)
			if recurse {
				oldPrefix := log.Prefix()
				log.SetPrefix(oldPrefix + "        ")
				printNodes(osmCache, nil, way.Refs, false)
				log.SetPrefix(oldPrefix)
			}
		}
		if deps {
			oldPrefix := log.Prefix()
			log.SetPrefix(oldPrefix + "        ")
			rels := diffCache.Ways.Get(id)
			if len(rels) != 0 {
				printRelations(osmCache, rels, false)
			}
			log.SetPrefix(oldPrefix)
		}
	}
}

func printNodes(osmCache *cache.OSMCache, diffCache *cache.DiffCache, ids []int64, deps bool) {
	for _, id := range ids {
		node, err := osmCache.Nodes.GetNode(id)
		if err != cache.NotFound && err != nil {
			log.Fatal(err)
		}
		if node == nil {
			node, err = osmCache.Coords.GetCoord(id)
			if err == cache.NotFound {
				log.Println("node:", id, "not found")
			} else if err != nil {
				log.Fatal(err)
			}
		}
		if node != nil {
			log.Println("node:", node)
		}
		if deps {
			oldPrefix := log.Prefix()
			log.SetPrefix(oldPrefix + "        ")
			ways := diffCache.Coords.Get(id)
			if len(ways) != 0 {
				printWays(osmCache, diffCache, ways, false, true)
			}
			log.SetPrefix(oldPrefix)
		}
	}
}

func Usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s %s:\n\n", os.Args[0], os.Args[1])
	flags.PrintDefaults()
	fmt.Fprintln(os.Stderr, "\nQuery cache for nodes/ways/relations.")
	os.Exit(1)
}

func Query(args []string) {
	flags.Usage = Usage

	if len(args) == 0 {
		Usage()
	}

	err := flags.Parse(args)
	if err != nil {
		log.Fatal(err)
	}
	log.SetFlags(0)
	log.SetOutput(os.Stdout)

	osmCache := cache.NewOSMCache(*cachedir)
	err = osmCache.Open()
	if err != nil {
		log.Fatal(err)
	}
	diffCache := cache.NewDiffCache(*cachedir)
	err = diffCache.Open()
	if err != nil {
		log.Fatal(err)
	}

	if *full && *deps {
		log.Fatal("cannot use -full and -deps option together")
	}

	if *relId != -1 {
		printRelations(osmCache, []int64{*relId}, *full)
	}

	if *wayId != -1 {
		printWays(osmCache, diffCache, []int64{*wayId}, *full, *deps)
	}

	if *nodeId != -1 {
		printNodes(osmCache, diffCache, []int64{*nodeId}, *deps)
	}

}
