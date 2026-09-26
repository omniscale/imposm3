package cache

import "github.com/jmhodges/levigo"

// SyncBarrier makes previous writes to every importer/dependency LevelDB durable.
// Call only after buffered coordinates/references are flushed and writers joined.
// An empty synchronous batch syncs the existing WAL without adding a fake OSM key.
// This is a batch barrier, not an fsync for each edited object.
func SyncBarrier(osm *OSMCache, diff *DiffCache) error {
	options := levigo.NewWriteOptions()
	defer options.Close()
	options.SetSync(true)
	batch := levigo.NewWriteBatch()
	defer batch.Close()
	for _, c := range []*cache{
		&osm.Coords.cache, &osm.Nodes.cache, &osm.Ways.cache, &osm.Relations.cache,
		&diff.Coords.cache, &diff.CoordsRel.cache, &diff.Ways.cache,
	} {
		if err := c.db.Write(options, batch); err != nil {
			return err
		}
	}
	return nil
}
