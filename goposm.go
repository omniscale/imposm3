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
	"goposm/writer"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"
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

func parse(cache *cache.OSMCache, progress *stats.Statistics, tagmapping *mapping.Mapping, filename string) {
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
			m := tagmapping.WayTagFilter()
			for ws := range ways {
				if skipWays {
					continue
				}
				for _, w := range ws {
					m.Filter(w.Tags)
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
			m := tagmapping.RelationTagFilter()
			for rels := range relations {
				for _, r := range rels {
					m.Filter(r.Tags)
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
			m := tagmapping.NodeTagFilter()
			for nds := range nodes {
				if skipNodes {
					continue
				}
				for _, nd := range nds {
					ok := m.Filter(nd.Tags)
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
	memprofile     = flag.String("memprofile", "", "dir name of mem profile output and interval (fname:interval)")
	cachedir       = flag.String("cachedir", "/tmp/goposm", "cache directory")
	overwritecache = flag.Bool("overwritecache", false, "overwritecache")
	appendcache    = flag.Bool("appendcache", false, "append cache")
	read           = flag.String("read", "", "read")
	write          = flag.Bool("write", false, "write")
	connection     = flag.String("connection", "", "connection parameters")
	diff           = flag.Bool("diff", false, "enable diff support")
	mappingFile    = flag.String("mapping", "", "mapping file")
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

	if *memprofile != "" {
		parts := strings.Split(*memprofile, string(os.PathListSeparator))
		var interval time.Duration

		if len(parts) < 2 {
			interval, _ = time.ParseDuration("1m")
		} else {
			var err error
			interval, err = time.ParseDuration(parts[1])
			if err != nil {
				log.Fatal(err)
			}
		}

		go stats.MemProfiler(parts[0], interval)
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

	tagmapping, err := mapping.NewMapping(*mappingFile)
	if err != nil {
		log.Fatal(err)
	}

	if *read != "" {
		osmCache.Coords.SetLinearImport(true)
		parse(osmCache, progress, tagmapping, *read)
		osmCache.Coords.SetLinearImport(false)
		progress.Reset()
		osmCache.Coords.Flush()
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
		wayChan := make(chan []element.Way)
		waitDb := &sync.WaitGroup{}
		conf := db.Config{
			Type:             "postgres",
			ConnectionParams: *connection,
			Srid:             3857,
			Schema:           "public",
		}
		pg, err := db.Open(conf)
		if err != nil {
			log.Fatal(err)
		}

		err = pg.Init(tagmapping)
		if err != nil {
			log.Fatal(err)
		}
		writeDBChan := make(chan writer.InsertBatch)
		writeChan := make(chan writer.InsertElement)
		waitBuffer := sync.WaitGroup{}

		for i := 0; i < runtime.NumCPU(); i++ {
			waitDb.Add(1)
			go func() {
				writer.DBWriter(pg, writeDBChan)
				waitDb.Done()
			}()
		}

		waitBuffer.Add(1)
		go func() {
			writer.BufferInsertElements(writeChan, writeDBChan)
			waitBuffer.Done()
		}()

		for i := 0; i < runtime.NumCPU(); i++ {
			waitFill.Add(1)
			go func() {
				lineStrings := tagmapping.LineStringMatcher()
				polygons := tagmapping.PolygonMatcher()
				var err error
				geos := geos.NewGEOS()
				defer geos.Finish()

				for w := range way {
					progress.AddWays(1)
					ok := osmCache.Coords.FillWay(w)
					if !ok {
						continue
					}
					proj.NodesToMerc(w.Nodes)
					if matches := lineStrings.Match(w.OSMElem); len(matches) > 0 {
						way := element.Way{}
						way.Id = w.Id
						way.Tags = w.Tags
						way.Geom, err = geom.LineStringWKB(geos, w.Nodes)
						if err != nil {
							if err, ok := err.(ErrorLevel); ok {
								if err.Level() <= 0 {
									continue
								}
							}
							log.Println(err)
							continue
						}
						for _, match := range matches {
							row := match.Row(&way.OSMElem)
							writeChan <- writer.InsertElement{match.Table, row}
						}

					}
					if w.IsClosed() {
						if matches := polygons.Match(w.OSMElem); len(matches) > 0 {
							way := element.Way{}
							way.Id = w.Id
							way.Tags = w.Tags
							way.Geom, err = geom.PolygonWKB(geos, w.Nodes)
							if err != nil {
								if err, ok := err.(ErrorLevel); ok {
									if err.Level() <= 0 {
										continue
									}
								}
								log.Println(err)
								continue
							}
							for _, match := range matches {
								row := match.Row(&way.OSMElem)
								writeChan <- writer.InsertElement{match.Table, row}
							}
						}
					}

					if *diff {
						diffCache.Coords.AddFromWay(w)
					}
				}
				waitFill.Done()
			}()
		}
		waitFill.Wait()
		close(wayChan)
		close(writeChan)
		waitBuffer.Wait()
		close(writeDBChan)
		waitDb.Wait()
		diffCache.Coords.Close()
	}
	progress.Stop()

	//parser.PBFStats(os.Args[1])
}
