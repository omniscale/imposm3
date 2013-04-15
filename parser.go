package main

import (
	"fmt"
	"goposm/cache"
	"goposm/element"
	"goposm/parser"
	"os"
	"runtime"
	"sync"
)

func parse(filename string) {
	nodes := make(chan []element.Node)
	ways := make(chan []element.Way)
	relations := make(chan element.Relation)

	positions := parser.PBFBlockPositions(filename)

	waitParser := sync.WaitGroup{}
	for i := 0; i < 4; i++ {
		waitParser.Add(1)
		go func() {
			for pos := range positions {
				parser.ParseBlock(pos, nodes, ways, relations)
			}
			waitParser.Done()
		}()
	}

	waitCounter := sync.WaitGroup{}
	wayCache := cache.NewWaysCache("/tmp/goposm/way.cache")
	defer wayCache.Close()
	for i := 0; i < 2; i++ {
		waitCounter.Add(1)
		go func() {
			wayCounter := 0
			for ws := range ways {
				wayCache.PutWays(ws)
				wayCounter += 1
			}
			fmt.Println("ways", wayCounter)
			waitCounter.Done()
		}()
	}
	relCache := cache.NewRelationsCache("/tmp/goposm/relation.cache")
	defer relCache.Close()
	waitCounter.Add(1)
	go func() {
		relationCounter := 0
		for rel := range relations {
			relCache.PutRelation(&rel)
			relationCounter += 1
		}
		fmt.Println("relations", relationCounter)
		waitCounter.Done()
	}()

	nodeCache := cache.NewDeltaCoordsCache("/tmp/goposm/node.cache")
	defer nodeCache.Close()
	for i := 0; i < 2; i++ {
		waitCounter.Add(1)
		go func() {
			nodeCounter := 0
			for nds := range nodes {
				if len(nds) == 0 {
					continue
				}
				nodeCache.PutCoords(nds)
				nodeCounter += 1
			}
			fmt.Println("nodes", nodeCounter)
			waitCounter.Done()
		}()
	}
	/*
		nodeCache := cache.NewCache("/tmp/goposm/node.cache")
		defer nodeCache.Close()
		for i := 0; i < 2; i++ {
			waitCounter.Add(1)
			go func() {
				nodeCounter := 0
				for nds := range nodes {
					if len(nds) == 0 {
						continue
					}
					nodeCache.PutCoordsPacked(nds[0].Id/8196, nds)
					nodeCounter += 1
				}
				fmt.Println("nodes", nodeCounter)
				waitCounter.Done()
			}()
		}
	*/
	waitParser.Wait()
	close(nodes)
	close(ways)
	close(relations)
	waitCounter.Wait()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	parse(os.Args[1])
	//parser.PBFStats(os.Args[1])
	fmt.Println("done")
}
