package reader

import (
	"goposm/cache"
	"goposm/element"
	"goposm/mapping"
	"goposm/parser"
	"goposm/stats"
	"os"
	"runtime"
	"sync"
)

var skipCoords, skipNodes, skipWays bool

func init() {
	if os.Getenv("GOPOSM_SKIP_COORDS") != "" {
		skipCoords = true
	}
	if os.Getenv("GOPOSM_SKIP_NODES") != "" {
		skipNodes = true
	}
	if os.Getenv("GOPOSM_SKIP_WAYS") != "" {
		skipWays = true
	}
}

func ReadPbf(cache *cache.OSMCache, progress *stats.Statistics, tagmapping *mapping.Mapping, filename string) {
	nodes := make(chan []element.Node, 4)
	coords := make(chan []element.Node, 4)
	ways := make(chan []element.Way, 4)
	relations := make(chan []element.Relation, 4)

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
			waitParser.Done()
		}()
	}

	waitWriter := sync.WaitGroup{}

	for i := 0; i < runtime.NumCPU(); i++ {
		waitWriter.Add(1)
		go func() {
			m := tagmapping.WayTagFilter()
			for ws := range ways {
				if skipWays {
					continue
				}
				for i, _ := range ws {
					m.Filter(&ws[i].Tags)
				}
				cache.Ways.PutWays(ws)
				progress.AddWays(len(ws))
			}
			waitWriter.Done()
		}()
	}

	for i := 0; i < runtime.NumCPU(); i++ {
		waitWriter.Add(1)
		go func() {
			m := tagmapping.RelationTagFilter()
			for rels := range relations {
				for i, _ := range rels {
					m.Filter(&rels[i].Tags)
				}
				cache.Relations.PutRelations(rels)
				progress.AddRelations(len(rels))
			}
			waitWriter.Done()
		}()
	}

	for i := 0; i < runtime.NumCPU(); i++ {
		waitWriter.Add(1)
		go func() {
			for nds := range coords {
				if skipCoords {
					continue
				}
				cache.Coords.PutCoords(nds)
				progress.AddCoords(len(nds))
			}
			waitWriter.Done()
		}()
	}

	for i := 0; i < runtime.NumCPU(); i++ {
		waitWriter.Add(1)
		go func() {
			m := tagmapping.NodeTagFilter()
			for nds := range nodes {
				for i, _ := range nds {
					m.Filter(&nds[i].Tags)
				}
				cache.Nodes.PutNodes(nds)
				progress.AddNodes(len(nds))
			}
			waitWriter.Done()
		}()
	}

	waitParser.Wait()
	close(coords)
	close(nodes)
	close(ways)
	close(relations)
	waitWriter.Wait()
}
