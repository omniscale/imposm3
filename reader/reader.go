package reader

import (
	osmcache "imposm3/cache"
	"imposm3/element"
	"imposm3/geom/geos"
	"imposm3/geom/limit"
	"imposm3/logging"
	"imposm3/mapping"
	"imposm3/parser/pbf"
	"imposm3/stats"
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
	if os.Getenv("IMPOSM_SKIP_COORDS") != "" {
		skipCoords = true
	}
	if os.Getenv("IMPOSM_SKIP_NODES") != "" {
		skipNodes = true
	}
	if os.Getenv("IMPOSM_SKIP_WAYS") != "" {
		skipWays = true
	}
	nParser = int64(runtime.NumCPU())
	nWays = int64(runtime.NumCPU())
	nRels = int64(runtime.NumCPU())
	nNodes = int64(runtime.NumCPU())
	nCoords = int64(runtime.NumCPU())
	if procConf := os.Getenv("IMPOSM_READ_PROCS"); procConf != "" {
		parts := strings.Split(procConf, ":")
		nParser, _ = strconv.ParseInt(parts[0], 10, 32)
		nRels, _ = strconv.ParseInt(parts[1], 10, 32)
		nWays, _ = strconv.ParseInt(parts[2], 10, 32)
		nNodes, _ = strconv.ParseInt(parts[3], 10, 32)
		nCoords, _ = strconv.ParseInt(parts[3], 10, 32)
	}

}

func ReadPbf(cache *osmcache.OSMCache, progress *stats.Statistics,
	tagmapping *mapping.Mapping, pbfFile *pbf.Pbf,
	limiter *limit.Limiter,
) {
	nodes := make(chan []element.Node, 4)
	coords := make(chan []element.Node, 4)
	ways := make(chan []element.Way, 4)
	relations := make(chan []element.Relation, 4)

	withLimiter := false
	if limiter != nil {
		withLimiter = true
	}

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
				// TODO check withLimiter
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
				numWithTags := 0
				for i, _ := range rels {
					m.Filter(&rels[i].Tags)
					if len(rels[i].Tags) > 0 {
						numWithTags += 1
					}
				}
				// TODO check withLimiter
				cache.Relations.PutRelations(rels)
				progress.AddRelations(numWithTags)
			}
			waitWriter.Done()
		}()
	}

	for i := 0; int64(i) < nCoords; i++ {
		waitWriter.Add(1)
		go func() {
			g := geos.NewGeos()
			defer g.Finish()
			for nds := range coords {
				if skipCoords {
					continue
				}
				if withLimiter {
					for i, _ := range nds {
						if !limiter.IntersectsBuffer(g, nds[i].Long, nds[i].Lat) {
							nds[i].Id = osmcache.SKIP
						}
					}
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
			g := geos.NewGeos()
			defer g.Finish()
			m := tagmapping.NodeTagFilter()
			for nds := range nodes {
				numWithTags := 0
				for i, _ := range nds {
					m.Filter(&nds[i].Tags)
					if len(nds[i].Tags) > 0 {
						numWithTags += 1
					}
					if withLimiter && !limiter.IntersectsBuffer(g, nds[i].Long, nds[i].Lat) {
						nds[i].Id = osmcache.SKIP
					}
				}
				cache.Nodes.PutNodes(nds)
				progress.AddNodes(numWithTags)
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
