package database

import (
	"errors"
	"strings"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/mapping/config"
)

type Config struct {
	ConnectionParams string
	Srid             int
	ImportSchema     string
	ProductionSchema string
	BackupSchema     string
}

type DB interface {
	Begin() error
	End() error
	Abort() error
	Init() error
	Close() error
	Inserter
}

type BulkBeginner interface {
	BeginBulk() error
}

type Inserter interface {
	// InsertXxx inserts element of that type into the database.
	// element.Geom is set to that type.
	InsertPoint(osm.Element, geom.Geometry, []mapping.Match) error
	InsertLineString(osm.Element, geom.Geometry, []mapping.Match) error
	InsertPolygon(osm.Element, geom.Geometry, []mapping.Match) error
	InsertRelationMember(osm.Relation, osm.Member, int, geom.Geometry, []mapping.Match) error
}

type Deployer interface {
	Deploy() error
	RevertDeploy() error
	RemoveBackup() error
}

type Generalizer interface {
	Generalize() error
	EnableGeneralizeUpdates()
	GeneralizeUpdates() error
}

type Finisher interface {
	Finish() error
}

type Deleter interface {
	Delete(int64, []mapping.Match) error
}

type Optimizer interface {
	Optimize() error
}

type FullDB interface {
	DB
	Deleter
	Generalizer
	Optimizer
	Finisher
}

var databases map[string]func(Config, *config.Mapping) (DB, error)

func init() {
	databases = make(map[string]func(Config, *config.Mapping) (DB, error))
}

func Register(name string, f func(Config, *config.Mapping) (DB, error)) {
	databases[name] = f
}

func Open(conf Config, m *config.Mapping) (DB, error) {
	parts := strings.SplitN(conf.ConnectionParams, ":", 2)
	connectionType := parts[0]

	newFunc, ok := databases[connectionType]
	if !ok {
		return nil, errors.New("unsupported database type: " + connectionType)
	}

	db, err := newFunc(conf, m)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// nullDb is a dummy database that imports into /dev/null
type nullDb struct{}

func (n *nullDb) Init() error                                                        { return nil }
func (n *nullDb) Begin() error                                                       { return nil }
func (n *nullDb) End() error                                                         { return nil }
func (n *nullDb) Close() error                                                       { return nil }
func (n *nullDb) Abort() error                                                       { return nil }
func (n *nullDb) InsertPoint(osm.Element, geom.Geometry, []mapping.Match) error      { return nil }
func (n *nullDb) InsertLineString(osm.Element, geom.Geometry, []mapping.Match) error { return nil }
func (n *nullDb) InsertPolygon(osm.Element, geom.Geometry, []mapping.Match) error    { return nil }
func (n *nullDb) InsertRelationMember(osm.Relation, osm.Member, int, geom.Geometry, []mapping.Match) error {
	return nil
}

func newNullDb(conf Config, m *config.Mapping) (DB, error) {
	return &nullDb{}, nil
}

func init() {
	Register("null", newNullDb)
}
