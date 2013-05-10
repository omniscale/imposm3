package main

import (
	"flag"
	"goposm/cache"
	"goposm/db"
	"goposm/element"
	"goposm/geom"
	"goposm/geom/geos"
	"goposm/mapping"
	"goposm/parser"
	"goposm/proj"
	"goposm/stats"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
)

var skipCoords, skipNodes, skipWays bool
var dbImportBatchSize int64

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

	dbImportBatchSize, _ = strconv.ParseInt(
		os.Getenv("GOPOSM_DBIMPORT_BATCHSIZE"), 10, 32)

	if dbImportBatchSize == 0 {
		dbImportBatchSize = 4096
	}
}

type ErrorLevel interface {
	Level() int
}

func parse(cache *cache.OSMCache, progress *stats.Statistics, filename string) {
	nodes := make(chan []element.Node)
	coords := make(chan []element.Node)
	ways := make(chan []element.Way)
	relations := make(chan []element.Relation)

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
			//runtime.GC()
			waitParser.Done()
		}()
	}

	waitCounter := sync.WaitGroup{}

	for i := 0; i < runtime.NumCPU(); i++ {
		waitCounter.Add(1)
		go func() {
			for ws := range ways {
				if skipWays {
					continue
				}
				for _, w := range ws {
					mapping.WayTags.Filter(w.Tags)
				}
				cache.Ways.PutWays(ws)
				progress.AddWays(len(ws))
			}
			waitCounter.Done()
		}()
	}
	for i := 0; i < runtime.NumCPU(); i++ {
		waitCounter.Add(1)
		go func() {
			for rels := range relations {
				for _, r := range rels {
					mapping.RelationTags.Filter(r.Tags)
				}
				cache.Relations.PutRelations(rels)
				progress.AddRelations(len(rels))
			}
			waitCounter.Done()
		}()
	}
	for i := 0; i < runtime.NumCPU(); i++ {
		waitCounter.Add(1)
		go func() {
			for nds := range coords {
				if skipCoords {
					continue
				}
				cache.Coords.PutCoords(nds)
				progress.AddCoords(len(nds))
			}
			waitCounter.Done()
		}()
	}
	for i := 0; i < 2; i++ {
		waitCounter.Add(1)
		go func() {
			for nds := range nodes {
				if skipNodes {
					continue
				}
				for _, nd := range nds {
					ok := mapping.PointTags.Filter(nd.Tags)
					if !ok {
						nd.Tags = nil
					}
				}
				n, _ := cache.Nodes.PutNodes(nds)
				progress.AddNodes(n)
			}
			waitCounter.Done()
		}()
	}

	waitParser.Wait()
	close(coords)
	close(nodes)
	close(ways)
	close(relations)
	waitCounter.Wait()
}

var (
	cpuprofile     = flag.String("cpuprofile", "", "filename of cpu profile output")
	cachedir       = flag.String("cachedir", "/tmp/goposm", "cache directory")
	overwritecache = flag.Bool("overwritecache", false, "overwritecache")
	appendcache    = flag.Bool("appendcache", false, "append cache")
	read           = flag.String("read", "", "read")
	write          = flag.Bool("write", false, "write")
	connection     = flag.String("connection", "", "connection parameters")
	diff           = flag.Bool("diff", false, "enable diff support")
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	osmCache := cache.NewOSMCache(*cachedir)

	if *read != "" && osmCache.Exists() {
		if *overwritecache {
			log.Println("removing existing cache", *cachedir)
			err := osmCache.Remove()
			if err != nil {
				log.Fatal("unable to remove cache:", err)
			}
		} else if !*appendcache {
			log.Fatal("cache already exists use -appendcache or -overwritecache")
		}
	}

	err := osmCache.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer osmCache.Close()

	progress := stats.StatsReporter()

	if *read != "" {
		parse(osmCache, progress, *read)
		progress.Reset()
	}

	if *write {
		progress.Reset()
		rel := osmCache.Relations.Iter()
		for _ = range rel {
			progress.AddRelations(1)
			// fmt.Println(r)
		}

		way := osmCache.Ways.Iter()

		diffCache := cache.NewDiffCache(*cachedir)
		if err = diffCache.Remove(); err != nil {
			log.Fatal(err)
		}
		if err = diffCache.Open(); err != nil {
			log.Fatal(err)
		}

		waitFill := sync.WaitGroup{}
		waitDiff := sync.WaitGroup{}
		wayChan := make(chan []element.Way)
		diffChan := make(chan *element.Way)
		waitDb := &sync.WaitGroup{}
		config := db.Config{"postgres", *connection, 3857, "public"}
		pg, err := db.Open(config)
		if err != nil {
			log.Fatal(err)
		}
		specs := []db.TableSpec{
			{
				"goposm_test",
				config.Schema,
				[]db.ColumnSpec{
					{"name", "VARCHAR"},
					{"highway", "VARCHAR"},
				},
				"LINESTRING",
				config.Srid,
			},
		}
		err = pg.Init(specs)
		if err != nil {
			log.Fatal(err)
		}
		for i := 0; i < runtime.NumCPU(); i++ {
			waitDb.Add(1)
			go func() {
				for ways := range wayChan {
					err = pg.InsertWays(ways, specs[0])
					if err != nil {
						log.Fatal(err)
					}
				}
				waitDb.Done()
			}()
		}

		for i := 0; i < runtime.NumCPU(); i++ {
			waitDiff.Add(1)
			go func() {
				for way := range diffChan {
					for _, node := range way.Nodes {
						diffCache.Coords.Add(node.Id, way.Id)
					}
				}
				waitDiff.Done()
			}()
		}

		for i := 0; i < runtime.NumCPU(); i++ {
			waitFill.Add(1)
			go func() {
				geos := geos.NewGEOS()
				defer geos.Finish()

				batch := make([]element.Way, 0, dbImportBatchSize)
				for w := range way {
					progress.AddWays(1)
					ok := osmCache.Coords.FillWay(w)
					if !ok {
						continue
					}
					proj.NodesToMerc(w.Nodes)
					w.Wkb, err = geom.LineStringWKB(geos, w.Nodes)
					if err != nil {
						if err, ok := err.(ErrorLevel); ok {
							if err.Level() <= 0 {
								continue
							}
						}
						log.Println(err)
						continue
					}
					batch = append(batch, *w)

					if len(batch) >= int(dbImportBatchSize) {
						wayChan <- batch
						batch = make([]element.Way, 0, dbImportBatchSize)
					}

					if *diff {
						diffChan <- w
					}
				}
				wayChan <- batch
				waitFill.Done()
			}()
		}
		waitFill.Wait()
		close(wayChan)
		close(diffChan)
		waitDiff.Wait()
		waitDb.Wait()
	}
	progress.Stop()

	//parser.PBFStats(os.Args[1])
}
