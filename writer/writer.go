package writer

import (
	"runtime"
	"sync"

	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/database"
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/expire"
	"github.com/omniscale/imposm3/geom/limit"
	"github.com/omniscale/imposm3/logging"
	"github.com/omniscale/imposm3/proj"
	"github.com/omniscale/imposm3/stats"
)

var log = logging.NewLogger("writer")

type ErrorLevel interface {
	Level() int
}

type looper interface {
	loop()
}

type OsmElemWriter struct {
	osmCache   *cache.OSMCache
	diffCache  *cache.DiffCache
	progress   *stats.Statistics
	inserter   database.Inserter
	wg         *sync.WaitGroup
	limiter    *limit.Limiter
	writer     looper
	srid       int
	expireor   expire.Expireor
	concurrent bool
}

func (writer *OsmElemWriter) SetLimiter(limiter *limit.Limiter) {
	writer.limiter = limiter
}

func (writer *OsmElemWriter) EnableConcurrent() {
	writer.concurrent = true
}

func (writer *OsmElemWriter) Start() {
	concurrency := 1
	if writer.concurrent {
		concurrency = runtime.NumCPU()
	}
	for i := 0; i < concurrency; i++ {
		writer.wg.Add(1)
		go writer.writer.loop()
	}
}

func (writer *OsmElemWriter) SetExpireor(exp expire.Expireor) {
	writer.expireor = exp
}

func (writer *OsmElemWriter) Wait() {
	writer.wg.Wait()
}

func (writer *OsmElemWriter) NodesToSrid(nodes []element.Node) {
	if writer.srid == 4326 {
		return
	}
	if writer.srid != 3857 && writer.srid != 900913 {
		panic("invalid srid. only 4326, 3857 and 900913 are supported")
	}

	for i, nd := range nodes {
		nodes[i].Long, nodes[i].Lat = proj.WgsToMerc(nd.Long, nd.Lat)
	}
}

func (writer *OsmElemWriter) NodeToSrid(node *element.Node) {
	if writer.srid == 4326 {
		return
	}
	if writer.srid != 3857 && writer.srid != 900913 {
		panic("invalid srid. only 4326, 3857 and 900913 are supported")
	}
	node.Long, node.Lat = proj.WgsToMerc(node.Long, node.Lat)
}
