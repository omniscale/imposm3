package main

import (
	"flag"
	"fmt"
	"goposm/cache"
	"goposm/element"
	"goposm/parser"
	"goposm/stats"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
)

func parse(cache *cache.OSMCache, progress *stats.Statistics, filename string) {
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
				parser.ParseBlock(
					pos,
					coords,
					nodes,
					ways,
					relations,
				)
			}
			//runtime.GC()
			waitParser.Done()
		}()
	}

	waitCounter := sync.WaitGroup{}

	for i := 0; i < runtime.NumCPU(); i++ {
		waitCounter.Add(1)
		go func() {
			for ws := range ways {
				cache.Ways.PutWays(ws)
				progress.AddWays(len(ws))
			}
			waitCounter.Done()
		}()
	}
	for i := 0; i < runtime.NumCPU(); i++ {
		waitCounter.Add(1)
		go func() {
			for rels := range relations {
				cache.Relations.PutRelations(rels)
				progress.AddRelations(len(rels))
			}
			waitCounter.Done()
		}()
	}
	for i := 0; i < runtime.NumCPU(); i++ {
		waitCounter.Add(1)
		go func() {
			for nds := range coords {
				cache.Coords.PutCoords(nds)
				progress.AddCoords(len(nds))
			}
			waitCounter.Done()
		}()
	}
	for i := 0; i < 2; i++ {
		waitCounter.Add(1)
		go func() {
			for nds := range nodes {
				n, _ := cache.Nodes.PutNodes(nds)
				progress.AddNodes(n)
			}
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
	if true {
		f, err := os.Create("/tmp/goposm.pprof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	log.SetFlags(log.LstdFlags | log.Llongfile)
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	osmCache, err := cache.NewOSMCache("/tmp/goposm")
	if err != nil {
		log.Fatal(err)
	}
	defer osmCache.Close()

	fmt.Println("start")
	progress := stats.StatsReporter()

	// parse(osmCache, progress, flag.Arg(0))

	//rel := osmCache.Relations.Iter()
	//for r := range rel {
	//fmt.Println(r)
	//}

	way := osmCache.Ways.Iter()
	refCache, err := cache.NewRefIndex("/tmp/refindex")
	if err != nil {
		log.Fatal(err)
	}

	waitFill := sync.WaitGroup{}
	for i := 0; i < runtime.NumCPU(); i++ {
		waitFill.Add(1)

		go func() {
			for w := range way {
				progress.AddWays(-1)
				ok := osmCache.Coords.FillWay(w)
				if !ok {
					continue
				}
				if true {
					for _, node := range w.Nodes {
						refCache.Add(node.Id, w.Id)
					}
				}
			}
			waitFill.Done()
		}()
	}
	waitFill.Wait()
	//parser.PBFStats(os.Args[1])
	fmt.Println("done")
}
