package update

import (
	"fmt"
	"io"

	"github.com/pkg/errors"

	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/config"
	"github.com/omniscale/imposm3/database"
	_ "github.com/omniscale/imposm3/database/postgis"
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/expire"
	"github.com/omniscale/imposm3/geom/geos"
	"github.com/omniscale/imposm3/geom/limit"
	"github.com/omniscale/imposm3/log"
	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/parser/diff"
	"github.com/omniscale/imposm3/stats"
	diffstate "github.com/omniscale/imposm3/update/state"
	"github.com/omniscale/imposm3/writer"
)

func Diff(baseOpts config.Base, files []string) {
	if baseOpts.Quiet {
		log.SetMinLevel(log.LInfo)
	}

	var geometryLimiter *limit.Limiter
	if baseOpts.LimitTo != "" {
		var err error
		step := log.Step("Reading limitto geometries")
		geometryLimiter, err = limit.NewFromGeoJSON(
			baseOpts.LimitTo,
			baseOpts.LimitToCacheBuffer,
			baseOpts.Srid,
		)
		if err != nil {
			log.Fatal("[fatal] Reading limitto geometry:", err)
		}
		step()
	}
	osmCache := cache.NewOSMCache(baseOpts.CacheDir)
	err := osmCache.Open()
	if err != nil {
		log.Fatal("[fatal] Opening OSM cache:", err)
	}
	defer osmCache.Close()

	diffCache := cache.NewDiffCache(baseOpts.CacheDir)
	err = diffCache.Open()
	if err != nil {
		log.Fatal("[fatal] Opening diff cache:", err)
	}

	var exp expire.Expireor

	if baseOpts.ExpireTilesDir != "" {
		tileexpire := expire.NewTileList(baseOpts.ExpireTilesZoom, baseOpts.ExpireTilesDir)
		exp = tileexpire
		defer func() {
			if err := tileexpire.Flush(); err != nil {
				log.Println("[error] Writing tile expire file:", err)
			}
		}()
	}

	for _, oscFile := range files {
		err := Update(baseOpts, oscFile, geometryLimiter, exp, osmCache, diffCache, false)
		if err != nil {
			osmCache.Close()
			diffCache.Close()
			log.Fatalf("[fatal] Unable to process %s: %v", oscFile, err)
		}
	}
	// explicitly Close since os.Exit prevents defers
	osmCache.Close()
	diffCache.Close()
}

func Update(
	baseOpts config.Base,
	oscFile string,
	geometryLimiter *limit.Limiter,
	expireor expire.Expireor,
	osmCache *cache.OSMCache,
	diffCache *cache.DiffCache,
	force bool,
) error {
	state, err := diffstate.FromOscGz(oscFile)
	if err != nil {
		return err
	}
	lastState, err := diffstate.ParseLastState(baseOpts.DiffDir)
	if err != nil {
		log.Printf("[warn] Parsing last state from %s: %s", baseOpts.DiffDir, err)
	}

	if lastState != nil && lastState.Sequence != 0 && state != nil && state.Sequence <= lastState.Sequence {
		if !force {
			log.Println("[warn] Skipping ", state, ", already imported")
			return nil
		}
	}

	defer log.Step(fmt.Sprintf("Processing %s", oscFile))()

	parser, err := diff.NewOscGzParser(oscFile)
	if err != nil {
		return err
	}

	tagmapping, err := mapping.FromFile(baseOpts.MappingFile)
	if err != nil {
		return err
	}

	dbConf := database.Config{
		ConnectionParams: baseOpts.Connection,
		Srid:             baseOpts.Srid,
		// we apply diff imports on the Production schema
		ImportSchema:     baseOpts.Schemas.Production,
		ProductionSchema: baseOpts.Schemas.Production,
		BackupSchema:     baseOpts.Schemas.Backup,
	}
	db, err := database.Open(dbConf, &tagmapping.Conf)
	if err != nil {
		return errors.Wrap(err, "opening database")
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
		tagmapping.Conf.SingleIdSpace,
		tagmapping.PointMatcher,
		tagmapping.LineStringMatcher,
		tagmapping.PolygonMatcher,
		tagmapping.RelationMatcher,
		tagmapping.RelationMemberMatcher,
	)
	deleter.SetExpireor(expireor)

	progress := stats.NewStatsReporter()

	relTagFilter := tagmapping.RelationTagFilter()
	wayTagFilter := tagmapping.WayTagFilter()
	nodeTagFilter := tagmapping.NodeTagFilter()

	relations := make(chan *element.Relation)
	ways := make(chan *element.Way)
	nodes := make(chan *element.Node)

	relWriter := writer.NewRelationWriter(osmCache, diffCache,
		tagmapping.Conf.SingleIdSpace,
		relations,
		db, progress,
		tagmapping.PolygonMatcher,
		tagmapping.RelationMatcher,
		tagmapping.RelationMemberMatcher,
		baseOpts.Srid)
	relWriter.SetLimiter(geometryLimiter)
	relWriter.SetExpireor(expireor)
	relWriter.Start()

	wayWriter := writer.NewWayWriter(osmCache, diffCache,
		tagmapping.Conf.SingleIdSpace,
		ways, db,
		progress,
		tagmapping.PolygonMatcher,
		tagmapping.LineStringMatcher,
		baseOpts.Srid)
	wayWriter.SetLimiter(geometryLimiter)
	wayWriter.SetExpireor(expireor)
	wayWriter.Start()

	nodeWriter := writer.NewNodeWriter(osmCache, nodes, db,
		progress,
		tagmapping.PointMatcher,
		baseOpts.Srid)
	nodeWriter.SetLimiter(geometryLimiter)
	nodeWriter.SetExpireor(expireor)
	nodeWriter.Start()

	nodeIds := make(map[int64]struct{})
	wayIds := make(map[int64]struct{})
	relIds := make(map[int64]struct{})

	step := log.Step("Parsing changes, updating cache and removing elements")

	g := geos.NewGeos()

	for {
		elem, err := parser.Next()
		if err == io.EOF {
			break // finished
		}
		if err != nil {
			return errors.Wrap(err, "parsing diff")
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
		if err := deleter.Delete(elem); err != nil && err != cache.NotFound {
			return errors.Wrapf(err, "delete element %#v", elem)
		}
		if elem.Del {
			// no new or modified elem -> remove from cache
			if elem.Rel != nil {
				if err := osmCache.Relations.DeleteRelation(elem.Rel.Id); err != nil && err != cache.NotFound {
					return errors.Wrapf(err, "delete relation %v", elem.Rel)
				}
			} else if elem.Way != nil {
				if err := osmCache.Ways.DeleteWay(elem.Way.Id); err != nil && err != cache.NotFound {
					return errors.Wrapf(err, "delete way %v", elem.Way)
				}
				if err := diffCache.Ways.Delete(elem.Way.Id); err != nil && err != cache.NotFound {
					return errors.Wrapf(err, "delete way references %v", elem.Way)
				}
			} else if elem.Node != nil {
				if err := osmCache.Nodes.DeleteNode(elem.Node.Id); err != nil && err != cache.NotFound {
					return errors.Wrapf(err, "delete node %v", elem.Node)
				}
				if err := osmCache.Coords.DeleteCoord(elem.Node.Id); err != nil && err != cache.NotFound {
					return errors.Wrapf(err, "delete coord %v", elem.Node)
				}
			}
		}
		if elem.Mod && elem.Node != nil && elem.Node.Tags == nil {
			// handle modifies where a node drops all tags
			if err := osmCache.Nodes.DeleteNode(elem.Node.Id); err != nil && err != cache.NotFound {
				return errors.Wrapf(err, "delete node %v", elem.Node)
			}
		}
		if elem.Add || elem.Mod {
			if elem.Rel != nil {
				// check if first member is cached to avoid caching
				// unneeded relations (typical outside of our coverage)
				cached, err := osmCache.FirstMemberIsCached(elem.Rel.Members)
				if err != nil {
					return errors.Wrapf(err, "query first member %v", elem.Rel)
				}
				if cached {
					err := osmCache.Relations.PutRelation(elem.Rel)
					if err != nil {
						return errors.Wrapf(err, "put relation %v", elem.Rel)
					}
					relIds[elem.Rel.Id] = struct{}{}
				}
			} else if elem.Way != nil {
				// check if first coord is cached to avoid caching
				// unneeded ways (typical outside of our coverage)
				cached, err := osmCache.Coords.FirstRefIsCached(elem.Way.Refs)
				if err != nil {
					return errors.Wrapf(err, "query first ref %v", elem.Way)
				}
				if cached {
					err := osmCache.Ways.PutWay(elem.Way)
					if err != nil {
						return errors.Wrapf(err, "put way %v", elem.Way)
					}
					wayIds[elem.Way.Id] = struct{}{}
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
						return errors.Wrapf(err, "put node %v", elem.Node)
					}
					err = osmCache.Coords.PutCoords([]element.Node{*elem.Node})
					if err != nil {
						return errors.Wrapf(err, "put coord %v", elem.Node)
					}
					nodeIds[elem.Node.Id] = struct{}{}
				}
			}
		}
	}

	// mark member ways from deleted relations for re-insert
	for id, _ := range deleter.DeletedMemberWays() {
		wayIds[id] = struct{}{}
	}

	progress.Stop()
	step()
	step = log.Step("Importing added/modified elements")

	progress = stats.NewStatsReporter()

	// mark depending ways for (re)insert
	for nodeId, _ := range nodeIds {
		dependers := diffCache.Coords.Get(nodeId)
		for _, way := range dependers {
			wayIds[way] = struct{}{}
		}
	}

	// mark depending relations for (re)insert
	for nodeId, _ := range nodeIds {
		dependers := diffCache.CoordsRel.Get(nodeId)
		for _, rel := range dependers {
			relIds[rel] = struct{}{}
		}
	}
	for wayId, _ := range wayIds {
		dependers := diffCache.Ways.Get(wayId)
		// mark depending relations for (re)insert
		for _, rel := range dependers {
			relIds[rel] = struct{}{}
		}
	}

	for relId, _ := range relIds {
		rel, err := osmCache.Relations.GetRelation(relId)
		if err != nil {
			if err != cache.NotFound {
				return errors.Wrapf(err, "fetching cached relation %v", relId)
			}
			continue
		}
		// insert new relation
		progress.AddRelations(1)
		relations <- rel
	}

	for wayId, _ := range wayIds {
		way, err := osmCache.Ways.GetWay(wayId)
		if err != nil {
			if err != cache.NotFound {
				return errors.Wrapf(err, "fetching cached way %v", wayId)
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
				return errors.Wrapf(err, "fetching cached node %v", nodeId)
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

	step()

	progress.Stop()

	if state != nil {
		if lastState != nil {
			state.Url = lastState.Url
		}
		err = diffstate.WriteLastState(baseOpts.DiffDir, state)
		if err != nil {
			log.Println("[error] Unable to write last state:", err)
		}
	}
	return nil
}
