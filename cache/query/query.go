package query

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	"goposm/cache"
	"goposm/element"
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

type nodes map[string]*node
type ways map[string]*way
type relations map[string]*relation

type node struct {
	element.Node
	Ways ways `json:"ways,omitempty"`
}

type way struct {
	element.Way
	Nodes     nodes     `json:"nodes,omitempty"`
	Relations relations `json:"relations,omitempty"`
}

type relation struct {
	element.Relation
	Ways ways `json:"ways,omitempty"`
}

type result struct {
	Nodes     nodes     `json:"nodes,omitempty"`
	Ways      ways      `json:"ways,omitempty"`
	Relations relations `json:"relations,omitempty"`
}

func collectRelations(osmCache *cache.OSMCache, ids []int64, recurse bool) relations {
	rels := make(relations)
	for _, id := range ids {
		sid := strconv.FormatInt(id, 10)
		rel, err := osmCache.Relations.GetRelation(id)
		if err == cache.NotFound {
			rels[sid] = nil
		} else if err != nil {
			log.Fatal(err)
		} else {
			rels[sid] = &relation{*rel, nil}
			if recurse {
				memberWayIds := []int64{}
				for _, m := range rel.Members {
					if m.Type == element.WAY {
						memberWayIds = append(memberWayIds, m.Id)
					}
				}
				rels[sid].Ways = collectWays(osmCache, nil, memberWayIds, true, false)

			}
		}
	}
	return rels
}

func collectWays(osmCache *cache.OSMCache, diffCache *cache.DiffCache, ids []int64, recurse, deps bool) ways {
	ws := make(ways)
	for _, id := range ids {
		sid := strconv.FormatInt(id, 10)
		w, err := osmCache.Ways.GetWay(id)
		if err == cache.NotFound {
			ws[sid] = nil
		} else if err != nil {
			log.Fatal(err)
		} else {
			ws[sid] = &way{*w, nil, nil}
			if recurse {
				ws[sid].Nodes = collectNodes(osmCache, nil, w.Refs, false)
			}
			if deps {
				rels := diffCache.Ways.Get(id)
				if len(rels) != 0 {
					ws[sid].Relations = collectRelations(osmCache, rels, false)
				}
			}
		}
	}
	return ws
}

func collectNodes(osmCache *cache.OSMCache, diffCache *cache.DiffCache, ids []int64, deps bool) nodes {
	ns := make(nodes)
	for _, id := range ids {
		sid := strconv.FormatInt(id, 10)
		n, err := osmCache.Nodes.GetNode(id)
		if err != cache.NotFound && err != nil {
			log.Fatal(err)
		}
		if n == nil {
			n, err = osmCache.Coords.GetCoord(id)
			if err == cache.NotFound {
				ns[sid] = nil
			} else if err != nil {
				log.Fatal(err)
			}
		}
		if n != nil {
			ns[sid] = &node{*n, nil}
			if deps {
				ways := diffCache.Coords.Get(id)
				if len(ways) != 0 {
					ns[sid].Ways = collectWays(osmCache, diffCache, ways, false, true)
				}
			}
		}
	}
	return ns
}

func Usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s %s:\n\n", os.Args[0], os.Args[1])
	flags.PrintDefaults()
	fmt.Fprintln(os.Stderr, "\nQuery cache for nodes/ways/relations.")
	os.Exit(1)
}

func printJson(obj interface{}) {
	bytes, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(bytes))
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

	result := result{}

	if *relId != -1 {
		result.Relations = collectRelations(osmCache, []int64{*relId}, *full)
	}

	if *wayId != -1 {
		result.Ways = collectWays(osmCache, diffCache, []int64{*wayId}, *full, *deps)
	}

	if *nodeId != -1 {
		result.Nodes = collectNodes(osmCache, diffCache, []int64{*nodeId}, *deps)
	}

	printJson(result)
}
