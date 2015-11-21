package diff

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"

	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/config"
	"github.com/omniscale/imposm3/database"
	_ "github.com/omniscale/imposm3/database/postgis"
	"github.com/omniscale/imposm3/diff/parser"
	diffstate "github.com/omniscale/imposm3/diff/state"
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/expire"
	"github.com/omniscale/imposm3/geom/geos"
	"github.com/omniscale/imposm3/geom/limit"
	"github.com/omniscale/imposm3/logging"
	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/stats"
	"github.com/omniscale/imposm3/writer"
)

var log = logging.NewLogger("diff")

func Update(oscFile string, geometryLimiter *limit.Limiter, expireor expire.Expireor, osmCache *cache.OSMCache, diffCache *cache.DiffCache, force bool) error {
	state, err := diffstate.ParseFromOsc(oscFile)
	if err != nil {
		return err
	}
	lastState, err := diffstate.ParseLastState(config.BaseOptions.DiffDir)
	if err != nil {
		log.Warn(err)
	}

	if lastState != nil && lastState.Sequence != 0 && state != nil && state.Sequence <= lastState.Sequence {
		if !force {
			log.Warn(state, " already imported")
			return nil
		}
	}

	defer log.StopStep(log.StartStep(fmt.Sprintf("Processing %s", oscFile)))

	elems, errc := parser.Parse(oscFile)

	tagmapping, err := mapping.NewMapping(config.BaseOptions.MappingFile)
	if err != nil {
		return err
	}

	dbConf := database.Config{
		ConnectionParams: config.BaseOptions.Connection,
		Srid:             config.BaseOptions.Srid,
		// we apply diff imports on the Production schema
		ImportSchema:     config.BaseOptions.Schemas.Production,
		ProductionSchema: config.BaseOptions.Schemas.Production,
		BackupSchema:     config.BaseOptions.Schemas.Backup,
	}
	db, err := database.Open(dbConf, tagmapping)
	if err != nil {
		return errors.New("database open: " + err.Error())
	}
	defer db.Close()

	err = db.Begin()
	if err != nil {
		return err
	}

	delDb, ok := db.(database.Deleter)
	if !ok {
		return errors.New("database not deletable")
	}

	genDb, ok := db.(database.Generalizer)
	if ok {
		genDb.EnableGeneralizeUpdates()
	}

	deleter := NewDeleter(
		delDb,
		osmCache,
		diffCache,
		tagmapping.SingleIdSpace,
		tagmapping.PointMatcher(),
		tagmapping.LineStringMatcher(),
		tagmapping.PolygonMatcher(),
	)

	progress := stats.NewStatsReporter()

	relTagFilter := tagmapping.RelationTagFilter()
	wayTagFilter := tagmapping.WayTagFilter()
	nodeTagFilter := tagmapping.NodeTagFilter()

	relations := make(chan *element.Relation)
	ways := make(chan *element.Way)
	nodes := make(chan *element.Node)

	relWriter := writer.NewRelationWriter(osmCache, diffCache,
		tagmapping.SingleIdSpace,
		relations,
		db, progress,
		tagmapping.PolygonMatcher(),
		tagmapping.LineStringMatcher(),
		config.BaseOptions.Srid)
	relWriter.SetLimiter(geometryLimiter)
	relWriter.SetExpireor(expireor)
	relWriter.Start()

	wayWriter := writer.NewWayWriter(osmCache, diffCache,
		tagmapping.SingleIdSpace,
		ways, db,
		progress,
		tagmapping.PolygonMatcher(),
		tagmapping.LineStringMatcher(),
		config.BaseOptions.Srid)
	wayWriter.SetLimiter(geometryLimiter)
	wayWriter.SetExpireor(expireor)
	wayWriter.Start()

	nodeWriter := writer.NewNodeWriter(osmCache, nodes, db,
		progress,
		tagmapping.PointMatcher(),
		config.BaseOptions.Srid)
	nodeWriter.SetLimiter(geometryLimiter)
	nodeWriter.SetExpireor(expireor)
	nodeWriter.Start()

	nodeIds := make(map[int64]bool)
	wayIds := make(map[int64]bool)
	relIds := make(map[int64]bool)

	step := log.StartStep("Parsing changes, updating cache and removing elements")

	g := geos.NewGeos()

	for {
		select {
		case elem, ok := <-elems:
			if !ok {
				elems = nil
				break
			}
			if elem.Rel != nil {
				relTagFilter.Filter(&elem.Rel.Tags)
				progress.AddRelations(1)
			} else if elem.Way != nil {
				wayTagFilter.Filter(&elem.Way.Tags)
				progress.AddWays(1)
			} else if elem.Node != nil {
				nodeTagFilter.Filter(&elem.Node.Tags)
				if len(elem.Node.Tags) > 0 {
					progress.AddNodes(1)
				}
				progress.AddCoords(1)
			}

			// always delete, to prevent duplicate elements from overlap of initial
			// import and diff import
			if err := deleter.Delete(elem); err != nil {
				return diffError(err, "delete element", elem)
			}
			if elem.Del {
				if !elem.Add {
					// no new or modified elem -> remove from cache
					if elem.Rel != nil {
						if err := osmCache.Relations.DeleteRelation(elem.Rel.Id); err != nil && err != cache.NotFound {
							return diffError(err, "delete relation %v", elem.Rel)
						}
					} else if elem.Way != nil {
						if err := osmCache.Ways.DeleteWay(elem.Way.Id); err != nil && err != cache.NotFound {
							return diffError(err, "delete way %v", elem.Way)
						}
						if err := diffCache.Ways.Delete(elem.Way.Id); err != nil && err != cache.NotFound {
							return diffError(err, "delete way references %v", elem.Way)
						}
					} else if elem.Node != nil {
						if err := osmCache.Nodes.DeleteNode(elem.Node.Id); err != nil && err != cache.NotFound {
							return diffError(err, "delete node %v", elem.Node)
						}
						if err := osmCache.Coords.DeleteCoord(elem.Node.Id); err != nil && err != cache.NotFound {
							return diffError(err, "delete coord %v", elem.Node)
						}
					}
				} else if elem.Node != nil && elem.Node.Tags == nil {
					// handle modifies where a node drops all tags
					if err := osmCache.Nodes.DeleteNode(elem.Node.Id); err != nil && err != cache.NotFound {
						return diffError(err, "delete node %v", elem.Node)
					}
				}
			}
			if elem.Add {
				if elem.Rel != nil {
					// check if first member is cached to avoid caching
					// unneeded relations (typical outside of our coverage)
					cached, err := osmCache.Ways.FirstMemberIsCached(elem.Rel.Members)
					if err != nil {
						return diffError(err, "query first member %v", elem.Rel)
					}
					if cached {
						err := osmCache.Relations.PutRelation(elem.Rel)
						if err != nil {
							return diffError(err, "put relation %v", elem.Rel)
						}
						relIds[elem.Rel.Id] = true
					}
				} else if elem.Way != nil {
					// check if first coord is cached to avoid caching
					// unneeded ways (typical outside of our coverage)
					cached, err := osmCache.Coords.FirstRefIsCached(elem.Way.Refs)
					if err != nil {
						return diffError(err, "query first ref %v", elem.Way)
					}
					if cached {
						err := osmCache.Ways.PutWay(elem.Way)
						if err != nil {
							return diffError(err, "put way %v", elem.Way)
						}
						wayIds[elem.Way.Id] = true
					}
				} else if elem.Node != nil {
					addNode := true
					if geometryLimiter != nil {
						if !geometryLimiter.IntersectsBuffer(g, elem.Node.Long, elem.Node.Lat) {
							addNode = false
						}
					}
					if addNode {
						err := osmCache.Nodes.PutNode(elem.Node)
						if err != nil {
							return diffError(err, "put node %v", elem.Node)
						}
						err = osmCache.Coords.PutCoords([]element.Node{*elem.Node})
						if err != nil {
							return diffError(err, "put coord %v", elem.Node)
						}
						nodeIds[elem.Node.Id] = true
					}
				}
			}
		case err, ok := <-errc:
			if !ok {
				errc = nil
				break
			}
			return diffError(err, "")
		}
		if errc == nil && elems == nil {
			break
		}
	}

	// mark member ways from deleted relations for re-insert
	for id, _ := range deleter.DeletedMemberWays() {
		wayIds[id] = true
	}

	progress.Stop()
	log.StopStep(step)
	step = log.StartStep("Writing added/modified elements")

	progress = stats.NewStatsReporter()

	// mark depending ways for (re)insert
	for nodeId, _ := range nodeIds {
		dependers := diffCache.Coords.Get(nodeId)
		for _, way := range dependers {
			wayIds[way] = true
		}
	}

	// mark depending relations for (re)insert
	for wayId, _ := range wayIds {
		dependers := diffCache.Ways.Get(wayId)
		// mark depending relations for (re)insert
		for _, rel := range dependers {
			relIds[rel] = true
		}
	}

	for relId, _ := range relIds {
		rel, err := osmCache.Relations.GetRelation(relId)
		if err != nil {
			if err != cache.NotFound {
				return diffError(err, "could not get relation %v", relId)
			}
			continue
		}
		// insert new relation
		progress.AddRelations(1)
		// filter out unsupported relation types, otherwise they might
		// get inserted with the tags from an outer way
		if relTagFilter.Filter(&rel.Tags) {
			relations <- rel
		}
	}

	for wayId, _ := range wayIds {
		way, err := osmCache.Ways.GetWay(wayId)
		if err != nil {
			if err != cache.NotFound {
				return diffError(err, "could not get way %v", wayId)
			}
			continue
		}
		// insert new way
		progress.AddWays(1)
		ways <- way
	}

	for nodeId, _ := range nodeIds {
		node, err := osmCache.Nodes.GetNode(nodeId)
		if err != nil {
			if err != cache.NotFound {
				return diffError(err, "could not get node %v", nodeId)
			}
			// missing nodes can still be Coords
			// no `continue` here
		}
		if node != nil {
			// insert new node
			progress.AddNodes(1)
			nodes <- node
		}
	}

	close(relations)
	close(ways)
	close(nodes)

	nodeWriter.Wait()
	relWriter.Wait()
	wayWriter.Wait()

	if genDb != nil {
		genDb.GeneralizeUpdates()
	}

	err = db.End()
	if err != nil {
		return err
	}
	err = db.Close()
	if err != nil {
		return err
	}

	log.StopStep(step)

	progress.Stop()

	if state != nil {
		if lastState != nil {
			state.Url = lastState.Url
		}
		err = diffstate.WriteLastState(config.BaseOptions.DiffDir, state)
		if err != nil {
			log.Warn(err) // warn only
		}
	}
	return nil
}

func diffError(err error, msg string, args ...interface{}) error {
	_, file, line, _ := runtime.Caller(1)
	return fmt.Errorf("diff process error (%s:%d): %s %v",
		filepath.Base(file), line, fmt.Sprintf(msg, args...), err)
}
