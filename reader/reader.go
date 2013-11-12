package reader

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	osmcache "imposm3/cache"
	"imposm3/element"
	"imposm3/geom/geos"
	"imposm3/geom/limit"
	"imposm3/logging"
	"imposm3/mapping"
	"imposm3/parser/pbf"
	"imposm3/proj"
	"imposm3/stats"
	"imposm3/util"
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

	parser := pbf.NewParser(pbfFile, coords, nodes, ways, relations)

	coordsSynced := make(chan bool)
	coordsSync := util.NewSyncPoint(int(nCoords+nNodes), func() {
		coordsSynced <- true
	})
	parser.NotifyWays(func() {
		for i := 0; int64(i) < nCoords; i++ {
			coords <- nil
		}
		for i := 0; int64(i) < nNodes; i++ {
			nodes <- nil
		}
		<-coordsSynced
	})

	waysSynced := make(chan bool)
	waysSync := util.NewSyncPoint(int(nWays), func() {
		waysSynced <- true
	})
	parser.NotifyRelations(func() {
		for i := 0; int64(i) < nWays; i++ {
			ways <- nil
		}
		<-waysSynced
	})

	parser.Start()

	waitWriter := sync.WaitGroup{}

	for i := 0; int64(i) < nWays; i++ {
		waitWriter.Add(1)
		go func() {
			var skip, hit int

			m := tagmapping.WayTagFilter()
			for ws := range ways {
				if ws == nil {
					waysSync.Sync()
					continue
				}
				if skipWays {
					continue
				}
				for i, _ := range ws {
					m.Filter(&ws[i].Tags)
					if withLimiter {
						if !cache.Coords.FirstRefIsCached(ws[i].Refs) {
							ws[i].Id = osmcache.SKIP
							skip += 1

						} else {
							hit += 1
						}
					}
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
			var skip, hit int

			m := tagmapping.RelationTagFilter()
			for rels := range relations {
				numWithTags := 0
				for i, _ := range rels {
					m.Filter(&rels[i].Tags)
					if len(rels[i].Tags) > 0 {
						numWithTags += 1
					}
					if withLimiter {
						if !cache.Ways.FirstMemberIsCached(rels[i].Members) {
							skip += 1

							rels[i].Id = osmcache.SKIP
						} else {
							hit += 1

						}
					}
				}
				cache.Relations.PutRelations(rels)
				progress.AddRelations(numWithTags)
			}

			waitWriter.Done()
		}()
	}

	for i := 0; int64(i) < nCoords; i++ {
		waitWriter.Add(1)
		go func() {
			var skip, hit int
			g := geos.NewGeos()
			defer g.Finish()
			for nds := range coords {
				if nds == nil {
					coordsSync.Sync()
					continue
				}
				if withLimiter {
					for i, _ := range nds {
						nd := element.Node{Long: nds[i].Long, Lat: nds[i].Lat}
						proj.NodeToMerc(&nd)
						if !limiter.IntersectsBuffer(g, nd.Long, nd.Lat) {
							skip += 1
							nds[i].Id = osmcache.SKIP
						} else {
							hit += 1
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
				if nds == nil {
					coordsSync.Sync()
					continue
				}
				numWithTags := 0
				for i, _ := range nds {
					m.Filter(&nds[i].Tags)
					if len(nds[i].Tags) > 0 {
						numWithTags += 1
					}
					if withLimiter {
						nd := element.Node{Long: nds[i].Long, Lat: nds[i].Lat}
						proj.NodeToMerc(&nd)
						if !limiter.IntersectsBuffer(g, nd.Long, nd.Lat) {
							nds[i].Id = osmcache.SKIP
						}
					}
				}
				cache.Nodes.PutNodes(nds)
				progress.AddNodes(numWithTags)
			}
			waitWriter.Done()
		}()
	}

	parser.Close()
	close(relations)
	close(ways)
	close(nodes)
	close(coords)
	waitWriter.Wait()
}
