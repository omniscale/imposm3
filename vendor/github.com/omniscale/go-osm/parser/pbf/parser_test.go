package pbf

import (
	"context"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/omniscale/go-osm"
)

func TestParser(t *testing.T) {
	checkParser(t, false)
}

func BenchmarkParser(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		checkParser(b, false)
	}
}

func BenchmarkParser_IncludeMetadata(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		checkParser(b, true)
	}
}

func checkParser(t testing.TB, includeMD bool) {
	conf := Config{
		Coords:          make(chan []osm.Node),
		Nodes:           make(chan []osm.Node),
		Ways:            make(chan []osm.Way),
		Relations:       make(chan []osm.Relation),
		IncludeMetadata: includeMD,
	}

	f, err := os.Open("./monaco-20150428.osm.pbf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	p := New(f, conf)

	wg := sync.WaitGroup{}

	var numNodes, numCoords, numWays, numRelations int64

	go func() {
		wg.Add(1)
		for nd := range conf.Nodes {
			numNodes += int64(len(nd))
		}
		wg.Done()
	}()

	go func() {
		wg.Add(1)
		for nd := range conf.Coords {
			numCoords += int64(len(nd))
		}
		wg.Done()
	}()

	go func() {
		wg.Add(1)
		for ways := range conf.Ways {
			numWays += int64(len(ways))
		}
		wg.Done()
	}()

	go func() {
		wg.Add(1)
		for rels := range conf.Relations {
			numRelations += int64(len(rels))
		}
		wg.Done()
	}()

	err = p.Parse(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if numCoords != 17233 {
		t.Error("parsed an unexpected number of coords:", numCoords)
	}
	if numNodes != 978 {
		t.Error("parsed an unexpected number of nodes:", numNodes)
	}
	if numWays != 2398 {
		t.Error("parsed an unexpected number of ways:", numWays)
	}
	if numRelations != 108 {
		t.Error("parsed an unexpected number of relations:", numRelations)
	}
}

func TestParseCoords(t *testing.T) {
	conf := Config{
		Coords: make(chan []osm.Node),
	}

	f, err := os.Open("./monaco-20150428.osm.pbf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	p := New(f, conf)

	wg := sync.WaitGroup{}

	var numCoords int64

	go func() {
		wg.Add(1)
		for nd := range conf.Coords {
			numCoords += int64(len(nd))
		}
		wg.Done()
	}()

	err = p.Parse(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if numCoords != 17233 {
		t.Error("parsed an unexpected number of coords:", numCoords)
	}
}

func TestParseNodes(t *testing.T) {
	conf := Config{
		Nodes: make(chan []osm.Node),
	}

	f, err := os.Open("./monaco-20150428.osm.pbf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	p := New(f, conf)

	wg := sync.WaitGroup{}

	var numNodes int64

	go func() {
		wg.Add(1)
		for nd := range conf.Nodes {
			numNodes += int64(len(nd))
		}
		wg.Done()
	}()

	err = p.Parse(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if numNodes != 17233 {
		t.Error("parsed an unexpected number of nodes:", numNodes)
	}
}

func TestParserNotify(t *testing.T) {
	conf := Config{
		Coords:    make(chan []osm.Node),
		Nodes:     make(chan []osm.Node),
		Ways:      make(chan []osm.Way),
		Relations: make(chan []osm.Relation),
	}

	f, err := os.Open("./monaco-20150428.osm.pbf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	nodesWg := sync.WaitGroup{} // use nodesWg to wait till all nodes are processed
	nodesProcessed := false
	conf.OnFirstWay = func() {
		// Send nil sentinal to nodes/coords goroutines to indicate that they
		// should call nodesWg.Done after processing.
		// Note: Need to send as many nils as there are goroutines.
		conf.Coords <- nil
		conf.Nodes <- nil
		nodesProcessed = true
		nodesWg.Wait()
	}

	waysWg := sync.WaitGroup{} // use waysWg to wait till all ways are processed
	waysProcessed := false
	conf.OnFirstRelation = func() {
		// Send nil sentinal to ways goroutines to indicate that they
		// should call waysWg.Done after processing.
		// Note: Need to send as many nils as there are goroutines.
		conf.Ways <- nil
		waysProcessed = true
		waysWg.Wait()
	}

	p := New(f, conf)

	wg := sync.WaitGroup{}

	var numNodes, numCoords, numWays, numRelations int64

	nodesWg.Add(1)
	go func() {
		wg.Add(1)
		for nd := range conf.Nodes {
			if nd == nil {
				nodesWg.Done()
				nodesWg.Wait()
				continue
			}
			numNodes += int64(len(nd))
		}
		wg.Done()
	}()

	nodesWg.Add(1)
	go func() {
		wg.Add(1)
		for nd := range conf.Coords {
			if nd == nil {
				nodesWg.Done()
				nodesWg.Wait()
				continue
			}
			numCoords += int64(len(nd))
		}
		wg.Done()
	}()

	waysWg.Add(1)
	go func() {
		wg.Add(1)
		for ways := range conf.Ways {
			if ways == nil {
				waysWg.Done()
				waysWg.Wait()
				continue
			}
			if !nodesProcessed {
				t.Fatal("received ways before all nodes were processed")
			}
			numWays += int64(len(ways))
		}
		wg.Done()
	}()

	go func() {
		wg.Add(1)
		for rels := range conf.Relations {
			if !nodesProcessed {
				t.Fatal("received relations before all nodes were processed")
			}
			if !waysProcessed {
				t.Fatal("received relations before all ways were processed")
			}
			numRelations += int64(len(rels))
		}
		wg.Done()
	}()

	err = p.Parse(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if numCoords != 17233 {
		t.Error("parsed an unexpected number of coords:", numCoords)
	}
	if numNodes != 978 {
		t.Error("parsed an unexpected number of nodes:", numNodes)
	}
	if numWays != 2398 {
		t.Error("parsed an unexpected number of ways:", numWays)
	}
	if numRelations != 108 {
		t.Error("parsed an unexpected number of relations:", numRelations)
	}
}

func TestParseCancel(t *testing.T) {
	conf := Config{
		Nodes:       make(chan []osm.Node),
		Ways:        make(chan []osm.Way),
		Relations:   make(chan []osm.Relation),
		Concurrency: 1,
	}

	f, err := os.Open("./monaco-20150428.osm.pbf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	p := New(f, conf)

	wg := sync.WaitGroup{}
	ctx, stop := context.WithCancel(context.Background())
	var numNodes, numWays, numRelations int64

	go func() {
		wg.Add(1)
		for nd := range conf.Nodes {
			numNodes += int64(len(nd))
			// stop after first parsed nodes
			stop()
		}
		wg.Done()
	}()
	go func() {
		wg.Add(1)
		for ways := range conf.Ways {
			numWays += int64(len(ways))
		}
		wg.Done()
	}()
	go func() {
		wg.Add(1)
		for rels := range conf.Relations {
			numRelations += int64(len(rels))
		}
		wg.Done()
	}()

	err = p.Parse(ctx)
	if err != context.Canceled {
		t.Fatal(err)
	}
	wg.Wait()

	// only two blocks of 8k nodes should be parsed before everything is stop()ed
	if numNodes != 16000 {
		t.Error("parsed an unexpected number of nodes:", numNodes)
	}
	if numWays != 0 {
		t.Error("parsed an unexpected number of ways:", numWays)
	}
	if numRelations != 0 {
		t.Error("parsed an unexpected number of relations:", numRelations)
	}
}

func TestParseMetadata(t *testing.T) {
	conf := Config{
		IncludeMetadata: true,
		Nodes:           make(chan []osm.Node),
		Ways:            make(chan []osm.Way),
		Relations:       make(chan []osm.Relation),
		Concurrency:     1,
	}

	f, err := os.Open("./monaco-20150428.osm.pbf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	p := New(f, conf)

	wg := sync.WaitGroup{}

	var nodes []osm.Node
	wg.Add(1)
	go func() {
		for nds := range conf.Nodes {
			nodes = append(nodes, nds...)
		}
		wg.Done()
	}()

	var ways []osm.Way
	wg.Add(1)
	go func() {
		for ws := range conf.Ways {
			ways = append(ways, ws...)
		}
		wg.Done()
	}()

	var rels []osm.Relation
	wg.Add(1)
	go func() {
		for rs := range conf.Relations {
			rels = append(rels, rs...)
		}
		wg.Done()
	}()

	err = p.Parse(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	for _, tc := range []struct {
		Idx  int
		Want osm.Node
	}{
		{Idx: 0,
			Want: osm.Node{
				Element: osm.Element{
					ID: 21911863,
					Metadata: &osm.Metadata{
						UserID:    378737,
						UserName:  "Scrup",
						Version:   5,
						Timestamp: time.Unix(1335970231, 0),
						Changeset: 11480240,
					},
				},
				Lat:  43.737012500000006,
				Long: 7.422028,
			},
		},
		{Idx: 2,
			Want: osm.Node{
				Element: osm.Element{
					ID:   21911886,
					Tags: osm.Tags{"crossing_ref": "zebra", "highway": "crossing"},
					Metadata: &osm.Metadata{
						UserID:    378737,
						UserName:  "Scrup",
						Version:   8,
						Timestamp: time.Unix(1335884779, 0),
						Changeset: 11470653,
					},
				},
				Lat:  43.737239900000006,
				Long: 7.423498500000001,
			},
		},
	} {
		if !reflect.DeepEqual(nodes[tc.Idx], tc.Want) {
			t.Errorf("unexpected node, got:\n%#v\n%#v\nwant:\n%#v\n%#v", nodes[tc.Idx], nodes[tc.Idx].Metadata, tc.Want, tc.Want.Metadata)
		}
	}

	for _, tc := range []struct {
		Idx  int
		Want osm.Way
	}{
		{Idx: 0,
			Want: osm.Way{
				Element: osm.Element{
					ID:   4097656,
					Tags: osm.Tags{"highway": "primary", "name": "Avenue Princesse Alice", "oneway": "yes"},
					Metadata: &osm.Metadata{
						UserID:    852996,
						UserName:  "Mg2",
						Version:   7,
						Timestamp: time.Unix(1417551724, 0),
						Changeset: 27187519,
					},
				},
				Refs: []int64{21912089, 1079750744, 2104793864, 1110560507, 21912093, 21912095, 1079751630, 21912097, 21912099},
			},
		},
		{Idx: 2,
			Want: osm.Way{
				Element: osm.Element{
					ID:   4224972,
					Tags: osm.Tags{"name": "Avenue des Papalins", "oneway": "yes", "highway": "residential"},
					Metadata: &osm.Metadata{
						UserID:    393883,
						UserName:  "fmalamaire",
						Version:   9,
						Timestamp: time.Unix(1368522546, 0),
						Changeset: 16122419,
					},
				},
				Refs: []int64{25177418, 25177397},
			},
		},
	} {
		if !reflect.DeepEqual(ways[tc.Idx], tc.Want) {
			t.Errorf("unexpected way, got:\n%#v\n%#v\nwant:\n%#v\n%#v", ways[tc.Idx], ways[tc.Idx].Metadata, tc.Want, tc.Want.Metadata)
		}
	}

	for _, tc := range []struct {
		Idx  int
		Want osm.Relation
	}{
		{Idx: 26,
			Want: osm.Relation{
				Element: osm.Element{
					ID:   1369631,
					Tags: osm.Tags{"type": "multipolygon"},
					Metadata: &osm.Metadata{
						UserID:    110263,
						UserName:  "werner2101",
						Version:   2,
						Timestamp: time.Unix(1298228849, 0),
						Changeset: 7346501,
					},
				},
				Members: []osm.Member{
					osm.Member{ID: 94452671, Type: 1, Role: "inner"},
					osm.Member{ID: 94452619, Type: 1, Role: "outer"},
				},
			},
		},
	} {
		if !reflect.DeepEqual(rels[tc.Idx], tc.Want) {
			t.Errorf("unexpected rel, got:\n%#v\n%#v\nwant:\n%#v\n%#v", rels[tc.Idx], rels[tc.Idx].Metadata, tc.Want, tc.Want.Metadata)
		}
	}
}
