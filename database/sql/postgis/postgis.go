package postgis

import (
	pq "github.com/lib/pq"
	"imposm3/database"
	"imposm3/database/sql"
	"imposm3/mapping"
	"strings"
)

func New(conf database.Config, m *mapping.Mapping) (database.DB, error) {
	db := &sql.SQLDB{}

	db.Tables = make(map[string]*sql.TableSpec)
	db.GeneralizedTables = make(map[string]*sql.GeneralizedTableSpec)

	db.NormalTableQueryBuilder = make(map[string]sql.NormalTableQueryBuilder)
	db.GenTableQueryBuilder = make(map[string]sql.GenTableQueryBuilder)

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

	db.Params = params
	err = db.Open()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func init() {
	database.Register("postgres", New)
	database.Register("postgis", New)
}
