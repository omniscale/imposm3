package pbf_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/omniscale/go-osm"
	"github.com/omniscale/go-osm/parser/pbf"
)

func Example() {
	// Open PBF file.
	f, err := os.Open("./monaco-20150428.osm.pbf")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Create channels for the parsed elements.
	nodes := make(chan []osm.Node)
	ways := make(chan []osm.Way)
	relations := make(chan []osm.Relation)

	// Initialize PBF parser.
	p := pbf.New(f, pbf.Config{
		Nodes:     nodes,
		Ways:      ways,
		Relations: relations,
	})

	// ==========================================
	// This is where you can place your own code.
	// This example only counts nodes, ways and relations.

	// We start a separate goroutine for each type. We use WaitGroup to make
	// sure all elements are processed before we return the results.

	// You can even start multiple goroutines for each type to distribute
	// processing accross multiple CPUs.

	var numNodes, numWays, numRelations int64
	wg := sync.WaitGroup{}

	go func() {
		wg.Add(1)
		for nds := range nodes {
			numNodes += int64(len(nds))
		}
		wg.Done()
	}()

	go func() {
		wg.Add(1)
		for ways := range ways {
			numWays += int64(len(ways))
		}
		wg.Done()
	}()

	go func() {
		wg.Add(1)
		for rels := range relations {
			numRelations += int64(len(rels))
		}
		wg.Done()
	}()
	// ==========================================

	// Create a new context. Can be used for cancelation, or timeouts.
	ctx := context.Background()

	// Start parsing.
	err = p.Parse(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Wait till our custom goroutines are finished.
	wg.Wait()

	fmt.Printf("parsed %d nodes, %d ways and %d relations\n", numNodes, numWays, numRelations)
	// Output: parsed 17233 nodes, 2398 ways and 108 relations
}
