package database

import (
	"errors"
	"imposm3/element"
	"imposm3/mapping"
	"strings"
)

type Config struct {
	ConnectionParams string
	Srid             int
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
	// ProbeXxx returns true if the element should be inserted.
	// The interface{} value is passed to InsertXxx when that element
	// gets inserted (can be used to pass a match object to the insert call).
	ProbePoint(element.OSMElem) (bool, interface{})
	ProbeLineString(element.OSMElem) (bool, interface{})
	ProbePolygon(element.OSMElem) (bool, interface{})
	// InsertXxx inserts element of that type into the database.
	// element.Geom is set to that type.
	InsertPoint(element.OSMElem, interface{})
	InsertLineString(element.OSMElem, interface{})
	InsertPolygon(element.OSMElem, interface{})
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

func (n *nullDb) Init() error                                         { return nil }
func (n *nullDb) Begin() error                                        { return nil }
func (n *nullDb) End() error                                          { return nil }
func (n *nullDb) Close() error                                        { return nil }
func (n *nullDb) Abort() error                                        { return nil }
func (n *nullDb) InsertPoint(element.OSMElem, interface{})            {}
func (n *nullDb) InsertLineString(element.OSMElem, interface{})       {}
func (n *nullDb) InsertPolygon(element.OSMElem, interface{})          {}
func (n *nullDb) ProbePoint(element.OSMElem) (bool, interface{})      { return true, nil }
func (n *nullDb) ProbeLineString(element.OSMElem) (bool, interface{}) { return true, nil }
func (n *nullDb) ProbePolygon(element.OSMElem) (bool, interface{})    { return true, nil }

func newNullDb(conf Config, m *mapping.Mapping) (DB, error) {
	return &nullDb{}, nil
}

func init() {
	Register("null", newNullDb)
}
