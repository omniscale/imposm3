package database

import (
	"errors"
	"strings"

	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/mapping"
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
	InsertPoint(element.OSMElem, geom.Geometry, []mapping.Match) error
	InsertLineString(element.OSMElem, geom.Geometry, []mapping.Match) error
	InsertPolygon(element.OSMElem, geom.Geometry, []mapping.Match) error
	InsertRelationMember(element.Relation, element.Member, geom.Geometry, []mapping.Match) error
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
	Inserter
	// Delete deletes ID from tables that matched ProbeXxx
	Delete(int64, interface{}) error
	// DeleteElem deletes element from all tables
	DeleteElem(element.OSMElem) error
}

type Optimizer interface {
	Optimize() error
}

var databases map[string]func(Config, *mapping.Mapping) (DB, error)

func init() {
	databases = make(map[string]func(Config, *mapping.Mapping) (DB, error))
}

func Register(name string, f func(Config, *mapping.Mapping) (DB, error)) {
	databases[name] = f
}

func Open(conf Config, m *mapping.Mapping) (DB, error) {
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

func (n *nullDb) Init() error                                                            { return nil }
func (n *nullDb) Begin() error                                                           { return nil }
func (n *nullDb) End() error                                                             { return nil }
func (n *nullDb) Close() error                                                           { return nil }
func (n *nullDb) Abort() error                                                           { return nil }
func (n *nullDb) InsertPoint(element.OSMElem, geom.Geometry, []mapping.Match) error      { return nil }
func (n *nullDb) InsertLineString(element.OSMElem, geom.Geometry, []mapping.Match) error { return nil }
func (n *nullDb) InsertPolygon(element.OSMElem, geom.Geometry, []mapping.Match) error    { return nil }
func (n *nullDb) InsertRelationMember(element.Relation, element.Member, geom.Geometry, []mapping.Match) error {
	return nil
}

func newNullDb(conf Config, m *mapping.Mapping) (DB, error) {
	return &nullDb{}, nil
}

func init() {
	Register("null", newNullDb)
}
