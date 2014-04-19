package postgis

import (
	pq "github.com/lib/pq"
	"imposm3/database"
	"imposm3/database/sql"
  sqld "database/sql"
	"imposm3/mapping"
	"strings"
  "runtime"
  "imposm3/logging"
  "fmt"
)

var log = logging.NewLogger("SQL")

func New(conf database.Config, m *mapping.Mapping) (database.DB, error) {
	db := &sql.SQLDB{}
  
	worker := int(runtime.NumCPU() / 2)
	if worker < 1 {
		worker = 1
	}
  
  db.Worker = worker
  db.BulkSupported = true

	db.Tables = make(map[string]*sql.TableSpec)
	db.GeneralizedTables = make(map[string]*sql.GeneralizedTableSpec)

	db.NormalTableQueryBuilder = make(map[string]sql.NormalTableQueryBuilder)
	db.GenTableQueryBuilder = make(map[string]sql.GenTableQueryBuilder)
  
  db.SdbTypes = NewSdbTypes()

	db.Config = conf

	db.QB = NewQueryBuilder()

	if strings.HasPrefix(db.Config.ConnectionParams, "postgis://") {
		db.Config.ConnectionParams = strings.Replace(
			db.Config.ConnectionParams,
			"postgis", "postgres", 1,
		)
	}

	params, err := pq.ParseURL(db.Config.ConnectionParams)
	if err != nil {
		return nil, err
	}
	params = disableDefaultSslOnLocalhost(params)
	db.Prefix = prefixFromConnectionParams(params)

	for name, table := range m.Tables {
		db.Tables[name] = sql.NewTableSpec(db, table)
	}

	for name, tableSpec := range db.Tables {
		db.NormalTableQueryBuilder[name] = NewNormalTableQueryBuilder(tableSpec)
	}

	for name, table := range m.GeneralizedTables {
		db.GeneralizedTables[name] = sql.NewGeneralizedTableSpec(db, table)
	}

	for name, genTableSpec := range db.GeneralizedTables {
		db.GenTableQueryBuilder[name] = NewGenTableQueryBuilder(genTableSpec)
	}

	db.PrepareGeneralizedTableSources()
	db.PrepareGeneralizations()

	db.PointTagMatcher = m.PointMatcher()
	db.LineStringTagMatcher = m.LineStringMatcher()
	db.PolygonTagMatcher = m.PolygonMatcher()
  
  db.Optimizer = Optimize

	db.Params = params
	err = Open(db)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func init() {
	database.Register("postgres", New)
	database.Register("postgis", New)
}

func Open(sdb *sql.SQLDB) error {
	var err error

	sdb.Db, err = sqld.Open("postgres", sdb.Params)
	if err != nil {
		return err
	}
	// check that the connection actually works
	err = sdb.Db.Ping()
	if err != nil {
		return err
	}
	return nil
}

func Optimize(sdb *sql.SQLDB) error {
  defer log.StopStep(log.StartStep(fmt.Sprintf("Clustering on geometry")))

  p := sql.NewWorkerPool(sdb.Worker, len(sdb.Tables)+len(sdb.GeneralizedTables))
  for _, tbl := range sdb.Tables {
  	tableName := tbl.FullName
  	table := tbl
  	p.In <- func() error {
  		return clusterTable(sdb, tableName, table.Srid, table.Columns)
  	}
  }
  for _, tbl := range sdb.GeneralizedTables {
  	tableName := tbl.FullName
  	table := tbl
  	p.In <- func() error {
  		return clusterTable(sdb, tableName, table.Source.Srid, table.Source.Columns)
  	}
  }

  err := p.Wait()
  if err != nil {
  	return err
  }

  return nil
}

func clusterTable(sdb *sql.SQLDB, tableName string, srid int, columns []sql.ColumnSpec) error {
	for _, col := range columns {
		if col.Type.Name() == "GEOMETRY" {
			step := log.StartStep(fmt.Sprintf("Indexing %s on geohash", tableName))
			sql := fmt.Sprintf(`CREATE INDEX "%s_geom_geohash" ON "%s"."%s" (ST_GeoHash(ST_Transform(ST_SetSRID(Box2D(%s), %d), 4326)))`,
				tableName, sdb.Config.ImportSchema, tableName, col.Name, srid)
			_, err := sdb.Db.Exec(sql)
			log.StopStep(step)
			if err != nil {
				return err
			}

			step = log.StartStep(fmt.Sprintf("Clustering %s on geohash", tableName))
			sql = fmt.Sprintf(`CLUSTER "%s_geom_geohash" ON "%s"."%s"`,
				tableName, sdb.Config.ImportSchema, tableName)
			_, err = sdb.Db.Exec(sql)
			log.StopStep(step)
			if err != nil {
				return err
			}
			break
		}
	}

	step := log.StartStep(fmt.Sprintf("Analysing %s", tableName))
	sql := fmt.Sprintf(`ANALYSE "%s"."%s"`,
		sdb.Config.ImportSchema, tableName)
	_, err := sdb.Db.Exec(sql)
	log.StopStep(step)
	if err != nil {
		return err
	}

	return nil
}