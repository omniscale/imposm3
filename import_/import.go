/*
Package import_ provides the import sub command initial imports.
*/
package import_

import (
	"os"

	"github.com/omniscale/imposm3/cache"
	"github.com/omniscale/imposm3/config"
	"github.com/omniscale/imposm3/database"
	_ "github.com/omniscale/imposm3/database/postgis"
	"github.com/omniscale/imposm3/geom/limit"
	"github.com/omniscale/imposm3/log"
	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/reader"
	"github.com/omniscale/imposm3/stats"
	"github.com/omniscale/imposm3/update/state"
	"github.com/omniscale/imposm3/writer"
)

func Import(importOpts config.Import) {
	baseOpts := importOpts.Base

	if (importOpts.Write || importOpts.Read != "") && (importOpts.RevertDeploy || importOpts.RemoveBackup) {
		log.Fatal("-revertdeploy and -removebackup not compatible with -read/-write")
	}

	if importOpts.RevertDeploy && (importOpts.RemoveBackup || importOpts.DeployProduction) {
		log.Fatal("-revertdeploy not compatible with -deployproduction/-removebackup")
	}

	var geometryLimiter *limit.Limiter
	if (importOpts.Write || importOpts.Read != "") && baseOpts.LimitTo != "" {
		var err error
		step := log.Step("Reading limitto geometries")
		geometryLimiter, err = limit.NewFromGeoJSON(
			baseOpts.LimitTo,
			baseOpts.LimitToCacheBuffer,
			baseOpts.Srid,
		)
		if err != nil {
			log.Fatal(err)
		}
		step()
	}

	tagmapping, err := mapping.FromFile(baseOpts.MappingFile)
	if err != nil {
		log.Fatal("[error] reading mapping file: ", err)
	}

	var db database.DB

	if importOpts.Write || importOpts.DeployProduction || importOpts.RevertDeploy || importOpts.RemoveBackup || importOpts.Optimize {
		if baseOpts.Connection == "" {
			log.Fatal("[error] missing connection option in configuration")
		}
		conf := database.Config{
			ConnectionParams: baseOpts.Connection,
			Srid:             baseOpts.Srid,
			ImportSchema:     baseOpts.Schemas.Import,
			ProductionSchema: baseOpts.Schemas.Production,
			BackupSchema:     baseOpts.Schemas.Backup,
		}
		db, err = database.Open(conf, &tagmapping.Conf)
		if err != nil {
			log.Fatal("[error] opening database: ", err)
		}
		defer db.Close()
	}

	osmCache := cache.NewOSMCache(baseOpts.CacheDir)

	if importOpts.Read != "" && osmCache.Exists() {
		if importOpts.Overwritecache {
			log.Printf("removing existing cache %s", baseOpts.CacheDir)
			err := osmCache.Remove()
			if err != nil {
				log.Fatal("unable to remove cache:", err)
			}
		} else if !importOpts.Appendcache {
			log.Fatal("cache already exists use -appendcache or -overwritecache")
		}
	}

	step := log.Step("Imposm")

	var elementCounts *stats.ElementCounts

	if importOpts.Read != "" {
		step := log.Step("Reading OSM data")
		err = osmCache.Open()
		if err != nil {
			log.Fatal("[error] opening cache files: ", err)
		}
		progress := stats.NewStatsReporter()

		if !importOpts.Appendcache {
			// enable optimization if we don't append to existing cache
			osmCache.Coords.SetLinearImport(true)
		}

		readLimiter := geometryLimiter
		if baseOpts.LimitToCacheBuffer == 0.0 {
			readLimiter = nil
		}

		err := reader.ReadPbf(importOpts.Read,
			osmCache,
			progress,
			tagmapping,
			readLimiter,
		)
		if err != nil {
			log.Fatal(err)
		}

		osmCache.Coords.SetLinearImport(false)
		elementCounts = progress.Stop()
		osmCache.Close()
		step()
		if importOpts.Diff {
			diffstate, err := state.FromPbf(importOpts.Read, baseOpts.DiffStateBefore, baseOpts.ReplicationUrl, baseOpts.ReplicationInterval)
			if err != nil {
				log.Println("[error] parsing diff state form PBF", err)
			} else if diffstate != nil {
				os.MkdirAll(baseOpts.DiffDir, 0755)
				err := state.WriteLastState(baseOpts.DiffDir, diffstate)
				if err != nil {
					log.Println("[error] writing last.state.txt: ", err)
				}
			}
		}
	}

	if importOpts.Write {
		importFinished := log.Step("Importing OSM data")
		writeFinished := log.Step("Writing OSM data")
		progress := stats.NewStatsReporterWithEstimate(elementCounts)

		err = db.Init()
		if err != nil {
			log.Fatal(err)
		}

		bulkDb, ok := db.(database.BulkBeginner)
		if ok {
			err = bulkDb.BeginBulk()
		} else {
			err = db.Begin()
		}
		if err != nil {
			log.Fatal(err)
		}

		var diffCache *cache.DiffCache
		if importOpts.Diff {
			diffCache = cache.NewDiffCache(baseOpts.CacheDir)
			if err = diffCache.Remove(); err != nil {
				log.Fatal(err)
			}
			if err = diffCache.Open(); err != nil {
				log.Fatal(err)
			}
		}

		err = osmCache.Open()
		if err != nil {
			log.Fatal(err)
		}
		if diffCache != nil {
			diffCache.Coords.SetLinearImport(true)
			diffCache.Ways.SetLinearImport(true)
		}
		osmCache.Coords.SetReadOnly(true)

		relations := osmCache.Relations.Iter()
		relWriter := writer.NewRelationWriter(osmCache, diffCache,
			tagmapping.Conf.SingleIdSpace,
			relations,
			db, progress,
			tagmapping.PolygonMatcher,
			tagmapping.RelationMatcher,
			tagmapping.RelationMemberMatcher,
			baseOpts.Srid,
		)
		relWriter.SetLimiter(geometryLimiter)
		relWriter.EnableConcurrent()
		relWriter.Start()
		relWriter.Wait() // blocks till the Relations.Iter() finishes
		osmCache.Relations.Close()

		ways := osmCache.Ways.Iter()
		wayWriter := writer.NewWayWriter(osmCache, diffCache,
			tagmapping.Conf.SingleIdSpace,
			ways, db,
			progress,
			tagmapping.PolygonMatcher,
			tagmapping.LineStringMatcher,
			baseOpts.Srid,
		)
		wayWriter.SetLimiter(geometryLimiter)
		wayWriter.EnableConcurrent()
		wayWriter.Start()
		wayWriter.Wait() // blocks till the Ways.Iter() finishes
		osmCache.Ways.Close()

		nodes := osmCache.Nodes.Iter()
		nodeWriter := writer.NewNodeWriter(osmCache, nodes, db,
			progress,
			tagmapping.PointMatcher,
			baseOpts.Srid,
		)
		nodeWriter.SetLimiter(geometryLimiter)
		nodeWriter.EnableConcurrent()
		nodeWriter.Start()
		nodeWriter.Wait() // blocks till the Nodes.Iter() finishes
		osmCache.Close()

		err = db.End()
		if err != nil {
			log.Fatal(err)
		}

		progress.Stop()

		if importOpts.Diff {
			diffCache.Close()
		}

		writeFinished()

		if db, ok := db.(database.Generalizer); ok {
			if err := db.Generalize(); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("database not generalizeable")
		}

		if db, ok := db.(database.Finisher); ok {
			if err := db.Finish(); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("database not finishable")
		}
		importFinished()
	}

	if importOpts.Optimize {
		if db, ok := db.(database.Optimizer); ok {
			if err := db.Optimize(); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("database not optimizable")
		}
	}

	if importOpts.DeployProduction {
		if db, ok := db.(database.Deployer); ok {
			if err := db.Deploy(); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("database not deployable")
		}
	}

	if importOpts.RevertDeploy {
		if db, ok := db.(database.Deployer); ok {
			if err := db.RevertDeploy(); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("database not deployable")
		}
	}

	if importOpts.RemoveBackup {
		if db, ok := db.(database.Deployer); ok {
			if err := db.RemoveBackup(); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("database not deployable")
		}
	}

	step()

}
