/*
Package import_ provides the import sub command initial imports.
*/
package import_

import (
	"os"
	"path"

	"imposm3/cache"
	"imposm3/config"
	"imposm3/database"
	_ "imposm3/database/sql/postgis"
  _ "imposm3/database/sql/spatialite"
	state "imposm3/diff/state"
	"imposm3/geom/limit"
	"imposm3/logging"
	"imposm3/mapping"
	"imposm3/parser/pbf"
	"imposm3/reader"
	"imposm3/stats"
	"imposm3/writer"
)

var log = logging.NewLogger("")

func Import() {
	if config.BaseOptions.Quiet {
		logging.SetQuiet(true)
	}

	if (config.ImportOptions.Write || config.ImportOptions.Read != "") && (config.ImportOptions.RevertDeploy || config.ImportOptions.RemoveBackup) {
		log.Fatal("-revertdeploy and -removebackup not compatible with -read/-write")
	}

	if config.ImportOptions.RevertDeploy && (config.ImportOptions.RemoveBackup || config.ImportOptions.DeployProduction) {
		log.Fatal("-revertdeploy not compatible with -deployproduction/-removebackup")
	}

	var geometryLimiter *limit.Limiter
	if (config.ImportOptions.Write || config.ImportOptions.Read != "") && config.BaseOptions.LimitTo != "" {
		var err error
		step := log.StartStep("Reading limitto geometries")
		geometryLimiter, err = limit.NewFromGeoJsonWithBuffered(
			config.BaseOptions.LimitTo,
			config.BaseOptions.LimitToCacheBuffer,
		)
		if err != nil {
			log.Fatal(err)
		}
		log.StopStep(step)
	}

	tagmapping, err := mapping.NewMapping(config.BaseOptions.MappingFile)
	if err != nil {
		log.Fatal("mapping file: ", err)
	}

	var db database.DB

	if config.ImportOptions.Write || config.ImportOptions.DeployProduction || config.ImportOptions.RevertDeploy || config.ImportOptions.RemoveBackup || config.ImportOptions.Optimize {
		if config.BaseOptions.Connection == "" {
			log.Fatal("missing connection option")
		}
		conf := database.Config{
			ConnectionParams: config.BaseOptions.Connection,
			Srid:             config.BaseOptions.Srid,
			ImportSchema:     config.BaseOptions.Schemas.Import,
			ProductionSchema: config.BaseOptions.Schemas.Production,
			BackupSchema:     config.BaseOptions.Schemas.Backup,
		}
		db, err = database.Open(conf, tagmapping)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()
	}

	osmCache := cache.NewOSMCache(config.BaseOptions.CacheDir)

	if config.ImportOptions.Read != "" && osmCache.Exists() {
		if config.ImportOptions.Overwritecache {
			log.Printf("removing existing cache %s", config.BaseOptions.CacheDir)
			err := osmCache.Remove()
			if err != nil {
				log.Fatal("unable to remove cache:", err)
			}
		} else if !config.ImportOptions.Appendcache {
			log.Fatal("cache already exists use -appendcache or -overwritecache")
		}
	}

	step := log.StartStep("Imposm")

	var elementCounts *stats.ElementCounts

	if config.ImportOptions.Read != "" {
		step := log.StartStep("Reading OSM data")
		err = osmCache.Open()
		if err != nil {
			log.Fatal(err)
		}
		progress := stats.NewStatsReporter()

		pbfFile, err := pbf.Open(config.ImportOptions.Read)
		if err != nil {
			log.Fatal(err)
		}

		osmCache.Coords.SetLinearImport(true)
		readLimiter := geometryLimiter
		if config.BaseOptions.LimitToCacheBuffer == 0.0 {
			readLimiter = nil
		}
		reader.ReadPbf(osmCache, progress, tagmapping,
			pbfFile, readLimiter)

		osmCache.Coords.SetLinearImport(false)
		elementCounts = progress.Stop()
		osmCache.Close()
		log.StopStep(step)
		diffstate := state.FromPbf(pbfFile)
		if diffstate != nil {
			os.MkdirAll(config.BaseOptions.DiffDir, 0755)
			err := diffstate.WriteToFile(path.Join(config.BaseOptions.DiffDir, "last.state.txt"))
			if err != nil {
				log.Print("error writing last.state.txt: ", err)
			}
		}
	}

	if config.ImportOptions.Write {
		stepImport := log.StartStep("Importing OSM data")
		stepWrite := log.StartStep("Writing OSM data")
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
		if config.ImportOptions.Diff {
			diffCache = cache.NewDiffCache(config.BaseOptions.CacheDir)
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
		relWriter := writer.NewRelationWriter(osmCache, diffCache, relations,
			db, progress, config.BaseOptions.Srid)
		relWriter.SetLimiter(geometryLimiter)
		relWriter.EnableConcurrent()
		relWriter.Start()
		relWriter.Wait() // blocks till the Relations.Iter() finishes
		osmCache.Relations.Close()

		ways := osmCache.Ways.Iter()
		wayWriter := writer.NewWayWriter(osmCache, diffCache, ways, db,
			progress, config.BaseOptions.Srid)
		wayWriter.SetLimiter(geometryLimiter)
		wayWriter.EnableConcurrent()
		wayWriter.Start()
		wayWriter.Wait() // blocks till the Ways.Iter() finishes
		osmCache.Ways.Close()

		nodes := osmCache.Nodes.Iter()
		nodeWriter := writer.NewNodeWriter(osmCache, nodes, db,
			progress, config.BaseOptions.Srid)
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

		if config.ImportOptions.Diff {
			diffCache.Close()
		}

		log.StopStep(stepWrite)

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
		log.StopStep(stepImport)
	}

	if config.ImportOptions.Optimize {
		if db, ok := db.(database.Optimizer); ok {
			if err := db.Optimize(); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("database not optimizable")
		}
	}

	if config.ImportOptions.DeployProduction {
		if db, ok := db.(database.Deployer); ok && db.IsDeploymentSupported() {
			if err := db.Deploy(); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("database not deployable")
		}
	}

	if config.ImportOptions.RevertDeploy {
		if db, ok := db.(database.Deployer); ok && db.IsDeploymentSupported() {
			if err := db.RevertDeploy(); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("database not deployable")
		}
	}

	if config.ImportOptions.RemoveBackup {
		if db, ok := db.(database.Deployer); ok && db.IsDeploymentSupported() {
			if err := db.RemoveBackup(); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("database not deployable")
		}
	}

	log.StopStep(step)

}
