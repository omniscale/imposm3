package reader

import (
	"context"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/go-osm/parser/pbf"
	osmcache "github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/geom/geos"
	"github.com/omniscale/imposm3/geom/limit"
	"github.com/omniscale/imposm3/log"
	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/stats"
	"github.com/pkg/errors"
)

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
	nodes := make(chan []osm.Node, 4)
	coords := make(chan []osm.Node, 4)
	ways := make(chan []osm.Way, 4)
	relations := make(chan []osm.Relation, 4)

	withLimiter := false
	if limiter != nil {
		withLimiter = true
	}

	config := pbf.Config{
		Coords:    coords,
		Nodes:     nodes,
		Ways:      ways,
		Relations: relations,
	}

	// wait for all coords/nodes to be processed before continuing with
	// ways. required for -limitto checks
	coordsSync := sync.WaitGroup{}
	config.OnFirstWay = func() {
		for i := 0; int64(i) < nCoords; i++ {
			coords <- nil
		}
		for i := 0; int64(i) < nNodes; i++ {
			nodes <- nil
		}
		coordsSync.Wait()
	}

	// wait for all ways to be processed before continuing with
	// relations. required for -limitto checks
	waysSync := sync.WaitGroup{}
	config.OnFirstRelation = func() {
		for i := 0; int64(i) < nWays; i++ {
			ways <- nil
		}
		waysSync.Wait()
	}

	f, err := os.Open(filename)
	if err != nil {
		return errors.Wrap(err, "opening PBF file")
	}
	defer f.Close()

	parser := pbf.New(f, config)
	header, err := parser.Header()
	if err != nil {
		return errors.Wrap(err, "parsing PBF header")
	}

	if header.Time.Unix() != 0 {
		log.Printf("[info] reading %s with data till %v", filename, header.Time.Local())
	}

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
				for i := range ws {
					m.Filter(&ws[i].Tags)
					if withLimiter {
						cached, err := cache.Coords.FirstRefIsCached(ws[i].Refs)
						if err != nil {
							log.Printf("[error] checking for cached refs of way %d: %v", ws[i].ID, err)
							cached = true // don't skip in case of error
						}
						if cached {
							hit++
						} else {
							ws[i].ID = osmcache.SKIP
							skip++
						}
					}
				}
				err := cache.Ways.PutWays(ws)
				if err != nil {
					log.Printf("[error] caching ways: %v", err)
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
				for i := range rels {
					m.Filter(&rels[i].Tags)
					if len(rels[i].Tags) > 0 {
						numWithTags++
					}
					if withLimiter {
						cached, err := cache.FirstMemberIsCached(rels[i].Members)
						if err != nil {
							log.Printf("[error] checking for cached members of relation %d: %v", rels[i].ID, err)
							cached = true // don't skip in case of error
						}
						if cached {
							hit++
						} else {
							skip++
							rels[i].ID = osmcache.SKIP
						}
					}
				}
				err := cache.Relations.PutRelations(rels)
				if err != nil {
					log.Printf("[error] caching relation: %v", err)
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
					for i := range nds {
						if !limiter.IntersectsBuffer(g, nds[i].Long, nds[i].Lat) {
							skip++
							nds[i].ID = osmcache.SKIP
						} else {
							hit++
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
				for i := range nds {
					m.Filter(&nds[i].Tags)
					if len(nds[i].Tags) > 0 {
						numWithTags++
					}
					if withLimiter {
						if !limiter.IntersectsBuffer(g, nds[i].Long, nds[i].Lat) {
							nds[i].ID = osmcache.SKIP
						}
					}
				}
				cache.Nodes.PutNodes(nds)
				progress.AddNodes(numWithTags)
			}
			waitWriter.Done()
		}()
	}
	ctx := context.Background()
	if err := parser.Parse(ctx); err != nil {
		return errors.Wrap(err, "parsing PBF")
	}
	waitWriter.Wait()

	return nil
}
