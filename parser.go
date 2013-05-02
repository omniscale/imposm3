package main

import (
	"flag"
	"fmt"
	"goposm/cache"
	"goposm/element"
	"goposm/parser"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
)

func parse(filename string) {
	nodes := make(chan []element.Node)
	coords := make(chan []element.Node)
	ways := make(chan []element.Way)
	relations := make(chan []element.Relation)

	positions := parser.PBFBlockPositions(filename)

	waitParser := sync.WaitGroup{}
	for i := 0; i < runtime.NumCPU(); i++ {
		waitParser.Add(1)
		go func() {
			for pos := range positions {
				parser.ParseBlock(pos, coords, nodes, ways, relations)
			}
			waitParser.Done()
		}()
	}

	waitCounter := sync.WaitGroup{}
	wayCache, err := cache.NewWaysCache("/tmp/goposm/way.cache")
	if err != nil {
		log.Fatal(err)
	}
	defer wayCache.Close()
	for i := 0; i < runtime.NumCPU(); i++ {
		waitCounter.Add(1)
		go func() {
			wayCounter := 0
			for ws := range ways {
				wayCache.PutWays(ws)
				wayCounter += len(ws)
			}
			fmt.Println("ways", wayCounter)
			waitCounter.Done()
		}()
	}
	relCache, err := cache.NewRelationsCache("/tmp/goposm/relation.cache")
	if err != nil {
		log.Fatal(err)
	}
	defer relCache.Close()
	for i := 0; i < runtime.NumCPU(); i++ {
		waitCounter.Add(1)
		go func() {
			relationCounter := 0
			for rels := range relations {
				relCache.PutRelations(rels)
				relationCounter += len(rels)
			}
			fmt.Println("relations", relationCounter)
			waitCounter.Done()
		}()
	}
	coordCache, err := cache.NewDeltaCoordsCache("/tmp/goposm/coords.cache")
	if err != nil {
		log.Fatal(err)
	}
	defer coordCache.Close()
	for i := 0; i < runtime.NumCPU(); i++ {
		waitCounter.Add(1)
		go func() {
			nodeCounter := 0
			for nds := range coords {
				coordCache.PutCoords(nds)
				nodeCounter += len(nds)
			}
			fmt.Println("coords", nodeCounter)
			waitCounter.Done()
		}()
	}
	nodeCache, err := cache.NewNodesCache("/tmp/goposm/node.cache")
	if err != nil {
		log.Fatal(err)
	}
	defer nodeCache.Close()
	for i := 0; i < 2; i++ {
		waitCounter.Add(1)
		go func() {
			nodeCounter := 0
			for nds := range nodes {
				n, _ := nodeCache.PutNodes(nds)
				nodeCounter += n
			}
			fmt.Println("nodes", nodeCounter)
			waitCounter.Done()
		}()
	}

	waitParser.Wait()
	close(coords)
	close(nodes)
	close(ways)
	close(relations)
	waitCounter.Wait()
}

func main() {
	f, err := os.Create("/tmp/goposm.pprof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
	log.SetFlags(log.LstdFlags | log.Llongfile)
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	//parse(flag.Arg(0))

	relCache, err := cache.NewRelationsCache("/tmp/goposm/relation.cache")
	if err != nil {
		log.Fatal(err)
	}
	defer relCache.Close()

	rel := relCache.Iter()
	for r := range rel {
		fmt.Println(r)
	}

	wayCache, err := cache.NewWaysCache("/tmp/goposm/way.cache")
	if err != nil {
		log.Fatal(err)
	}
	defer wayCache.Close()

	coordCache, err := cache.NewDeltaCoordsCache("/tmp/goposm/coords.cache")
	if err != nil {
		log.Fatal(err)
	}
	defer coordCache.Close()

	way := wayCache.Iter()
	i := 0
	for w := range way {
		i += 1
		coordCache.FillWay(w)
		//fmt.Println(i)
	}
	fmt.Println(i)
	//parser.PBFStats(os.Args[1])
	fmt.Println("done")
}
