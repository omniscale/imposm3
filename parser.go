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
	nodes := make(chan element.Node)
	ways := make(chan element.Way)
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
	waitCounter.Add(1)
	go func() {
		cache := cache.NewCache("/tmp/goposm/way.cache")
		defer cache.Close()

		wayCounter := 0
		for way := range ways {
			cache.PutWay(&way)
			wayCounter += 1
		}
		fmt.Println("ways", wayCounter)
		waitCounter.Done()
	}()
	waitCounter.Add(1)
	go func() {
		cache := cache.NewCache("/tmp/goposm/relation.cache")
		defer cache.Close()

		relationCounter := 0
		for rel := range relations {
			cache.PutRelation(&rel)
			relationCounter += 1
		}
		fmt.Println("relations", relationCounter)
		waitCounter.Done()
	}()

	cache := cache.NewCache("/tmp/goposm/node.cache")
	defer cache.Close()
	for i := 0; i < 4; i++ {
		waitCounter.Add(1)
		go func() {
			nodeCounter := 0
			for node := range nodes {
				cache.PutNode(&node)
				nodeCounter += 1
			}
			fmt.Println("nodes", nodeCounter)
			waitCounter.Done()
		}()
	}
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
