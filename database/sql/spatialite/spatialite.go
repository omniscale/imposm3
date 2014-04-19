package spatialite

import (
	sqld "database/sql"
	"fmt"
	"github.com/mattn/go-sqlite3"
	"imposm3/database"
	"imposm3/database/sql"
	"imposm3/logging"
	"imposm3/mapping"
	"strings"
)

var log = logging.NewLogger("SQL")

func New(conf database.Config, m *mapping.Mapping) (database.DB, error) {
	db := &sql.SQLDB{}

	db.Worker = 1
	db.BulkSupported = false

	db.Tables = make(map[string]*sql.TableSpec)
	db.GeneralizedTables = make(map[string]*sql.GeneralizedTableSpec)

	db.NormalTableQueryBuilder = make(map[string]sql.NormalTableQueryBuilder)
	db.GenTableQueryBuilder = make(map[string]sql.GenTableQueryBuilder)

	db.SdbTypes = NewSdbTypes()
	db.Config = conf
	db.QB = NewQueryBuilder()

	db.DeploymentSupported = false

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

	db.Optimizer = optimize

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

	// check if database needs to be initialized
	sql_stmt :=
		`SELECT EXISTS(
      SELECT 1
      WHERE EXISTS (
        SELECT * FROM sqlite_master WHERE type='table' and name='geometry_columns'
      ) AND EXISTS (
        SELECT * FROM sqlite_master WHERE type='table' and name='spatial_ref_sys')
     )`

	var exists bool
	row := sdb.Db.QueryRow(sql_stmt)
	err = row.Scan(&exists)

	if err != nil {
		return err
	}

	if !exists {
		sql_stmt = "SELECT InitSpatialMetaData();"
		_, err = sdb.Db.Exec(sql_stmt)

		if err != nil {
			return err
		}
	}

	if err != nil {
		return err
	}

	return nil
}

func analyze(sdb *sql.SQLDB, table string) error {
	fmt.Sprintf("ANALYZE %s")
	step := log.StartStep(fmt.Sprintf("Analysing %s", table))
	sql := fmt.Sprintf(`ANALYZE "%s"`, table)
	_, err := sdb.Db.Exec(sql)
	log.StopStep(step)
	if err != nil {
		return err
	}

	return nil
}

func optimize(sdb *sql.SQLDB) error {
	fmt.Sprintf("ANALYZE %s")

	for _, tbl := range sdb.Tables {
		err := analyze(sdb, tbl.FullName)
		if err != nil {
			return err
		}
	}

	for _, tbl := range sdb.GeneralizedTables {
		err := analyze(sdb, tbl.FullName)
		if err != nil {
			return err
		}
	}

	return nil
}
