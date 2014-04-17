package spatialite

import (
	"github.com/mattn/go-sqlite3"
  "os"
	"imposm3/database"
	"imposm3/database/sql"
  sqld "database/sql"
	"imposm3/mapping"
	"strings"
)

func New(conf database.Config, m *mapping.Mapping) (database.DB, error) {
	db := &sql.SQLDB{}
  
  db.Worker = 1
  db.BulkSupported = false

	db.Tables = make(map[string]*sql.TableSpec)
	db.GeneralizedTables = make(map[string]*sql.GeneralizedTableSpec)

	db.NormalTableQueryBuilder = make(map[string]sql.NormalTableQueryBuilder)
	db.GenTableQueryBuilder = make(map[string]sql.GenTableQueryBuilder)

	db.Config = conf

	db.QB = NewQueryBuilder()

	if strings.HasPrefix(db.Config.ConnectionParams, "spatialite://") {
		db.Config.ConnectionParams = strings.Replace(
			db.Config.ConnectionParams,
			"spatialite://", "", 1,
		)
	}

	for name, table := range m.Tables {
		db.Tables[name] = sql.NewTableSpec(db, table)
	}
  
	for name, table := range m.GeneralizedTables {
		db.GeneralizedTables[name] = sql.NewGeneralizedTableSpec(db, table)
	}
  
  db.PrepareGeneralizedTableSources()
  db.PrepareGeneralizations()

	for name, tableSpec := range db.Tables {
		db.NormalTableQueryBuilder[name] = NewNormalTableQueryBuilder(tableSpec)
	}

	for name, genTableSpec := range db.GeneralizedTables {
		db.GenTableQueryBuilder[name] = NewGenTableQueryBuilder(genTableSpec)
	}

	db.PointTagMatcher = m.PointMatcher()
	db.LineStringTagMatcher = m.LineStringMatcher()
	db.PolygonTagMatcher = m.PolygonMatcher()

  // TODO do we need this?
	// db.Params = params
	err := Open(db)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func init() {
	database.Register("spatialite", New)
}

func Open(sdb *sql.SQLDB) error {
  // TODO check if this is the correct position
	os.Remove(sdb.Config.ConnectionParams)

	// load spatialite extension
	sqld.Register("sqlite3_with_spatialite",
		&sqlite3.SQLiteDriver{
			Extensions: []string{"mod_spatialite"},
		})

	var err error

	sdb.Db, err = sqld.Open("sqlite3_with_spatialite", sdb.Config.ConnectionParams)
	if err != nil {
		return err
	}
  
	s := "SELECT InitSpatialMetaData();"
	row := sdb.Db.QueryRow(s)
	var exists bool
	err = row.Scan(&exists)
	if err != nil {
    return err
	}
	if exists {
		return nil
	}
  
  // TODO check if we need an option for this
  /*
  _, err = pg.Db.Exec("PRAGMA synchronous = OFF;")
	if err != nil {
		return err
	}
  _, err = pg.Db.Exec("PRAGMA journal_mode = MEMORY;")
	if err != nil {
		return err
	}
  _, err = pg.Db.Exec("PRAGMA page_size = 1000000;")
	if err != nil {
		return err
	}
  _, err = pg.Db.Exec("VACUUM;")
	if err != nil {
		return err
	}
  _, err = pg.Db.Exec("PRAGMA cache_size = 1000000;")
	if err != nil {
		return err
	}
  */
  
  // pg.tx, err = pg.Db.Begin()
  
	if err != nil {
		return err
	}
  
	return nil
}