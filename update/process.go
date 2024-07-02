package update

import (
	"context"
	"os"

	"github.com/pkg/errors"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/go-osm/parser/diff"
	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/database"
	_ "github.com/omniscale/imposm3/database/postgis"
	"github.com/omniscale/imposm3/expire"
	"github.com/omniscale/imposm3/geom/geos"
	"github.com/omniscale/imposm3/geom/limit"
	"github.com/omniscale/imposm3/log"
	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/stats"
	"github.com/omniscale/imposm3/writer"
)

func importDiffFile(
	oscFile string,
	db database.FullDB,
	tagmapping *mapping.Mapping,
	srid int,
	geometryLimiter *limit.Limiter,
	expireor expire.Expireor,
	osmCache *cache.OSMCache,
	diffCache *cache.DiffCache,
) error {
	diffs := make(chan osm.Diff)
	config := diff.Config{
		Diffs: diffs,
	}

	f, err := os.Open(oscFile)
	if err != nil {
		return errors.Wrap(err, "opening diff file")
	}
	defer f.Close()
	parser, err := diff.NewGZIP(f, config)
	if err != nil {
		return errors.Wrap(err, "initializing diff parser")
	}

	db.EnableGeneralizeUpdates()

	deleter := NewDeleter(
		db,
		osmCache,
		diffCache,
		tagmapping.Conf.SingleIDSpace,
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

	relations := make(chan *osm.Relation)
	ways := make(chan *osm.Way)
	nodes := make(chan *osm.Node)

	relWriter := writer.NewRelationWriter(osmCache, diffCache,
		tagmapping.Conf.SingleIDSpace,
		relations,
		db, progress,
		tagmapping.PolygonMatcher,
		tagmapping.RelationMatcher,
		tagmapping.RelationMemberMatcher,
		srid)
	relWriter.SetLimiter(geometryLimiter)
	relWriter.SetExpireor(expireor)
	relWriter.Start()

	wayWriter := writer.NewWayWriter(osmCache, diffCache,
		tagmapping.Conf.SingleIDSpace,
		ways, db,
		progress,
		tagmapping.PolygonMatcher,
		tagmapping.LineStringMatcher,
		srid)
	wayWriter.SetLimiter(geometryLimiter)
	wayWriter.SetExpireor(expireor)
	wayWriter.Start()

	nodeWriter := writer.NewNodeWriter(osmCache, nodes, db,
		progress,
		tagmapping.PointMatcher,
		srid)
	nodeWriter.SetLimiter(geometryLimiter)
	nodeWriter.SetExpireor(expireor)
	nodeWriter.Start()

	nodeIDs := make(map[int64]struct{})
	wayIDs := make(map[int64]struct{})
	relIDs := make(map[int64]struct{})

	step := log.Step("Parsing changes, updating cache and removing elements")

	g := geos.NewGeos()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // make sure parser is stopped if we return early with an error

	parseError := make(chan error)
	go func() {
		parseError <- parser.Parse(ctx)
	}()

	for elem := range diffs {
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
		if elem.Delete {
			// no new or modified elem -> remove from cache
			if elem.Rel != nil {
				if err := osmCache.Relations.DeleteRelation(elem.Rel.ID); err != nil && err != cache.NotFound {
					return errors.Wrapf(err, "delete relation %v", elem.Rel)
				}
			} else if elem.Way != nil {
				if err := osmCache.Ways.DeleteWay(elem.Way.ID); err != nil && err != cache.NotFound {
					return errors.Wrapf(err, "delete way %v", elem.Way)
				}
				if err := diffCache.Ways.Delete(elem.Way.ID); err != nil && err != cache.NotFound {
					return errors.Wrapf(err, "delete way references %v", elem.Way)
				}
			} else if elem.Node != nil {
				if err := osmCache.Nodes.DeleteNode(elem.Node.ID); err != nil && err != cache.NotFound {
					return errors.Wrapf(err, "delete node %v", elem.Node)
				}
				if err := osmCache.Coords.DeleteCoord(elem.Node.ID); err != nil && err != cache.NotFound {
					return errors.Wrapf(err, "delete coord %v", elem.Node)
				}
			}
		}
		if elem.Modify && elem.Node != nil && elem.Node.Tags == nil {
			// handle modifies where a node drops all tags
			if err := osmCache.Nodes.DeleteNode(elem.Node.ID); err != nil && err != cache.NotFound {
				return errors.Wrapf(err, "delete node %v", elem.Node)
			}
		}
		if elem.Create || elem.Modify {
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
					relIDs[elem.Rel.ID] = struct{}{}
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
					wayIDs[elem.Way.ID] = struct{}{}
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
					err = osmCache.Coords.PutCoords([]osm.Node{*elem.Node})
					if err != nil {
						return errors.Wrapf(err, "put coord %v", elem.Node)
					}
					nodeIDs[elem.Node.ID] = struct{}{}
				}
			}
		}
	}

	// mark member ways from deleted relations for re-insert
	for id := range deleter.DeletedMemberWays() {
		wayIDs[id] = struct{}{}
	}

	progress.Stop()
	step()

	err = <-parseError
	if err != nil {
		return errors.Wrapf(err, "parsing diff %s", oscFile)
	}

	step = log.Step("Importing added/modified elements")

	progress = stats.NewStatsReporter()

	// mark depending ways for (re)insert
	for nodeID := range nodeIDs {
		dependers := diffCache.Coords.Get(nodeID)
		for _, way := range dependers {
			wayIDs[way] = struct{}{}
		}
	}

	// mark depending relations for (re)insert
	for nodeID := range nodeIDs {
		dependers := diffCache.CoordsRel.Get(nodeID)
		for _, rel := range dependers {
			relIDs[rel] = struct{}{}
		}
	}
	for wayID := range wayIDs {
		dependers := diffCache.Ways.Get(wayID)
		// mark depending relations for (re)insert
		for _, rel := range dependers {
			relIDs[rel] = struct{}{}
		}
	}

	for relID := range relIDs {
		rel, err := osmCache.Relations.GetRelation(relID)
		if err != nil {
			if err != cache.NotFound {
				return errors.Wrapf(err, "fetching cached relation %v", relID)
			}
			continue
		}
		// insert new relation
		progress.AddRelations(1)
		relations <- rel
	}

	for wayID := range wayIDs {
		way, err := osmCache.Ways.GetWay(wayID)
		if err != nil {
			if err != cache.NotFound {
				return errors.Wrapf(err, "fetching cached way %v", wayID)
			}
			continue
		}
		// insert new way
		progress.AddWays(1)
		ways <- way
	}

	for nodeID := range nodeIDs {
		node, err := osmCache.Nodes.GetNode(nodeID)
		if err != nil {
			if err != cache.NotFound {
				return errors.Wrapf(err, "fetching cached node %v", nodeID)
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

	db.GeneralizeUpdates()

	progress.Stop()
	step()

	return nil
}
