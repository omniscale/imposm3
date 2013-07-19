package reader

import (
	"goposm/cache"
	"goposm/element"
	"goposm/logging"
	"goposm/mapping"
	"goposm/parser/pbf"
	"goposm/stats"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

var log = logging.NewLogger("reader")

var skipCoords, skipNodes, skipWays bool
var nParser, nWays, nRels, nNodes, nCoords int64

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
	nParser = int64(runtime.NumCPU())
	nWays = int64(runtime.NumCPU())
	nRels = int64(runtime.NumCPU())
	nNodes = int64(runtime.NumCPU())
	nCoords = int64(runtime.NumCPU())
	if procConf := os.Getenv("GOPOSM_READ_PROCS"); procConf != "" {
		parts := strings.Split(procConf, ":")
		nParser, _ = strconv.ParseInt(parts[0], 10, 32)
		nRels, _ = strconv.ParseInt(parts[1], 10, 32)
		nWays, _ = strconv.ParseInt(parts[2], 10, 32)
		nNodes, _ = strconv.ParseInt(parts[3], 10, 32)
		nCoords, _ = strconv.ParseInt(parts[3], 10, 32)
	}

}

func ReadPbf(cache *cache.OSMCache, progress *stats.Statistics, tagmapping *mapping.Mapping, pbfFile *pbf.Pbf) {
	nodes := make(chan []element.Node, 4)
	coords := make(chan []element.Node, 4)
	ways := make(chan []element.Way, 4)
	relations := make(chan []element.Relation, 4)

	if pbfFile.Header.Time.Unix() != 0 {
		log.Printf("reading %s with data till %v", pbfFile.Filename, pbfFile.Header.Time.Local())
	}

	blocks := pbfFile.BlockPositions()

	waitParser := sync.WaitGroup{}
	for i := 0; int64(i) < nParser; i++ {
		waitParser.Add(1)
		go func() {
			for block := range blocks {
				block.Parse(
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

	for i := 0; int64(i) < nWays; i++ {
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

	for i := 0; int64(i) < nRels; i++ {
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

	for i := 0; int64(i) < nCoords; i++ {
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

	for i := 0; int64(i) < nNodes; i++ {
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
