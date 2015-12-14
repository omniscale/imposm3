package pbf

import (
	"sync"
	"testing"

	"github.com/omniscale/imposm3/element"
)

func TestParser(t *testing.T) {
	nodes := make(chan []element.Node)
	coords := make(chan []element.Node)
	ways := make(chan []element.Way)
	relations := make(chan []element.Relation)
	pbf, err := Open("monaco-20150428.osm.pbf")
	if err != nil {
		t.Fatal(err)
	}
	p := NewParser(pbf, coords, nodes, ways, relations)

	wg := sync.WaitGroup{}

	var numNodes, numCoords, numWays, numRelations int64

	go func() {
		wg.Add(1)
		for nd := range nodes {
			numNodes += int64(len(nd))
		}
		wg.Done()
	}()

	go func() {
		wg.Add(1)
		for nd := range coords {
			numCoords += int64(len(nd))
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

	p.Parse()
	wg.Wait()

	if numCoords != 17233 {
		t.Error("parsed an unexpected number of coords:", numCoords)
	}

	if element.ParseDontAddOnlyCreatedByTag {
		if numNodes != 978 {
			t.Error("parsed an unexpected number of nodes:", numNodes)
		}
	} else {
		if numNodes != 980 {
			t.Error("parsed an unexpected number of nodes:", numNodes)
		}
	}

	if numWays != 2398 {
		t.Error("parsed an unexpected number of ways:", numWays)
	}
	if numRelations != 108 {
		t.Error("parsed an unexpected number of relations:", numRelations)
	}
}

func TestParseCoords(t *testing.T) {
	coords := make(chan []element.Node)

	pbf, err := Open("monaco-20150428.osm.pbf")
	if err != nil {
		t.Fatal(err)
	}
	p := NewParser(pbf, coords, nil, nil, nil)

	wg := sync.WaitGroup{}

	var numCoords int64

	go func() {
		wg.Add(1)
		for nd := range coords {
			numCoords += int64(len(nd))
		}
		wg.Done()
	}()

	p.Parse()
	wg.Wait()

	if numCoords != 17233 {
		t.Error("parsed an unexpected number of coords:", numCoords)
	}
}

func TestParserNotify(t *testing.T) {
	nodes := make(chan []element.Node)
	coords := make(chan []element.Node)
	ways := make(chan []element.Way)
	relations := make(chan []element.Relation)
	pbf, err := Open("monaco-20150428.osm.pbf")
	if err != nil {
		t.Fatal(err)
	}
	p := NewParser(pbf, coords, nodes, ways, relations)
	waysWg := sync.WaitGroup{}
	p.FinishedCoords(func() {
		waysWg.Add(1)
		coords <- nil
		nodes <- nil
		waysWg.Done()
		waysWg.Wait()
	})

	wg := sync.WaitGroup{}

	var numNodes, numCoords, numWays, numRelations int64

	waysWg.Add(1)
	go func() {
		wg.Add(1)
		for nd := range nodes {
			if nd == nil {
				waysWg.Done()
				waysWg.Wait()
				continue
			}
			numNodes += int64(len(nd))
		}
		wg.Done()
	}()

	waysWg.Add(1)
	go func() {
		wg.Add(1)
		for nd := range coords {
			if nd == nil {
				waysWg.Done()
				waysWg.Wait()
				continue
			}
			numCoords += int64(len(nd))
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

	p.Parse()
	wg.Wait()

	if numCoords != 17233 {
		t.Error("parsed an unexpected number of coords:", numCoords)
	}

	if element.ParseDontAddOnlyCreatedByTag {
		if numNodes != 978 {
			t.Error("parsed an unexpected number of nodes:", numNodes)
		}
	} else {
		if numNodes != 980 {
			t.Error("parsed an unexpected number of nodes:", numNodes)
		}
	}

	if numWays != 2398 {
		t.Error("parsed an unexpected number of ways:", numWays)
	}
	if numRelations != 108 {
		t.Error("parsed an unexpected number of relations:", numRelations)
	}
}

func TestBarrier(t *testing.T) {
	done := make(chan bool)
	check := int32(0)
	bar := newBarrier(func() {
		done <- true
		check = 1
	})
	bar.add(2)

	wait := func() {
		if check != 0 {
			panic("check set")
		}
		bar.doneWait()
		if check != 1 {
			panic("check not set")
		}
	}
	go wait()
	go wait()

	<-done

	// does not wait/block
	bar.doneWait()

}
