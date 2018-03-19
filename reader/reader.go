package reader

import (
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	osmcache "github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/geom/geos"
	"github.com/omniscale/imposm3/geom/limit"
	"github.com/omniscale/imposm3/logging"
	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/parser/pbf"
	"github.com/omniscale/imposm3/stats"
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
	if procConf := os.Getenv("IMPOSM_READ_PROCS"); procConf != "" {
		parts := strings.Split(procConf, ":")
		nParser, _ = strconv.ParseInt(parts[0], 10, 32)
		nRels, _ = strconv.ParseInt(parts[1], 10, 32)
		nWays, _ = strconv.ParseInt(parts[2], 10, 32)
		nNodes, _ = strconv.ParseInt(parts[3], 10, 32)
		nCoords, _ = strconv.ParseInt(parts[3], 10, 32)
	} else {
		nParser, nRels, nWays, nNodes, nCoords = readersForCpus(runtime.NumCPU())
	}
}

func readersForCpus(cpus int) (int64, int64, int64, int64, int64) {
	cpuf := float64(cpus)
	return int64(math.Ceil(cpuf * 0.75)), int64(math.Ceil(cpuf * 0.25)), int64(math.Ceil(cpuf * 0.25)), int64(math.Ceil(cpuf * 0.25)), int64(math.Ceil(cpuf * 0.25))
}

func ReadPbf(
	filename string,
	cache *osmcache.OSMCache,
	progress *stats.Statistics,
	tagmapping *mapping.Mapping,
	limiter *limit.Limiter,
) error {
	nodes := make(chan []element.Node, 4)
	coords := make(chan []element.Node, 4)
	ways := make(chan []element.Way, 4)
	relations := make(chan []element.Relation, 4)

	withLimiter := false
	if limiter != nil {
		withLimiter = true
	}

	parser, err := pbf.NewParser(filename)
	if err != nil {
		return err
	}

	if header := parser.Header(); header.Time.Unix() != 0 {
		log.Printf("reading %s with data till %v", filename, header.Time.Local())
	}

	// wait for all coords/nodes to be processed before continuing with
	// ways. required for -limitto checks
	coordsSync := sync.WaitGroup{}
	parser.RegisterFirstWayCallback(func() {
		for i := 0; int64(i) < nCoords; i++ {
			coords <- nil
		}
		for i := 0; int64(i) < nNodes; i++ {
			nodes <- nil
		}
		coordsSync.Wait()
	})

	// wait for all ways to be processed before continuing with
	// relations. required for -limitto checks
	waysSync := sync.WaitGroup{}
	parser.RegisterFirstRelationCallback(func() {
		for i := 0; int64(i) < nWays; i++ {
			ways <- nil
		}
		waysSync.Wait()
	})

	waitWriter := sync.WaitGroup{}

	for i := 0; int64(i) < nWays; i++ {
		waysSync.Add(1)
		waitWriter.Add(1)
		go func() {
			var skip, hit int

			m := tagmapping.WayTagFilter()
			for ws := range ways {
				if ws == nil {
					waysSync.Done()
					waysSync.Wait()
					continue
				}
				if skipWays {
					continue
				}
				for i, _ := range ws {
					m.Filter(&ws[i].Tags)
					if withLimiter {
						cached, err := cache.Coords.FirstRefIsCached(ws[i].Refs)
						if err != nil {
							log.Errorf("error while checking for cached refs of way %d: %v", ws[i].Id, err)
							cached = true // don't skip in case of error
						}
						if cached {
							hit += 1
						} else {
							ws[i].Id = osmcache.SKIP
							skip += 1
						}
					}
				}
				err := cache.Ways.PutWays(ws)
				if err != nil {
					log.Errorf("error while caching ways: %v", err)
				}
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
						cached, err := cache.FirstMemberIsCached(rels[i].Members)
						if err != nil {
							log.Errorf("error while checking for cached members of relation %d: %v", rels[i].Id, err)
							cached = true // don't skip in case of error
						}
						if cached {
							hit += 1
						} else {
							skip += 1
							rels[i].Id = osmcache.SKIP
						}
					}
				}
				err := cache.Relations.PutRelations(rels)
				if err != nil {
					log.Errorf("error while caching relation: %v", err)
				}
				progress.AddRelations(numWithTags)
			}

			waitWriter.Done()
		}()
	}

	for i := 0; int64(i) < nCoords; i++ {
		coordsSync.Add(1)
		waitWriter.Add(1)
		go func() {
			var skip, hit int
			g := geos.NewGeos()
			defer g.Finish()
			for nds := range coords {
				if nds == nil {
					coordsSync.Done()
					coordsSync.Wait()
					continue
				}
				if skipCoords {
					continue
				}
				if withLimiter {
					for i, _ := range nds {
						if !limiter.IntersectsBuffer(g, nds[i].Long, nds[i].Lat) {
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
		coordsSync.Add(1)
		waitWriter.Add(1)
		go func() {
			g := geos.NewGeos()
			defer g.Finish()
			m := tagmapping.NodeTagFilter()
			for nds := range nodes {
				if nds == nil {
					coordsSync.Done()
					coordsSync.Wait()
					continue
				}
				if skipNodes {
					continue
				}
				numWithTags := 0
				for i, _ := range nds {
					m.Filter(&nds[i].Tags)
					if len(nds[i].Tags) > 0 {
						numWithTags += 1
					}
					if withLimiter {
						if !limiter.IntersectsBuffer(g, nds[i].Long, nds[i].Lat) {
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

	parser.Parse(coords, nodes, ways, relations)
	close(nodes)
	close(coords)
	close(ways)
	close(relations)
	waitWriter.Wait()

	return nil
}
