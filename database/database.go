package database

import (
	"errors"
	"imposm3/element"
	"imposm3/mapping"
	"strings"
)

type Config struct {
	Type             string
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
	ProbePoint(element.OSMElem) (bool, interface{})
	ProbeLineString(element.OSMElem) (bool, interface{})
	ProbePolygon(element.OSMElem) (bool, interface{})
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
}

type Finisher interface {
	Finish() error
}

type Deleter interface {
	Delete(string, int64) error
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
	newFunc, ok := databases[conf.Type]
	if !ok {
		return nil, errors.New("unsupported database type: " + conf.Type)
	}

	db, err := newFunc(conf, m)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func ConnectionType(param string) string {
	parts := strings.SplitN(param, ":", 2)
	return parts[0]
}

type NullDb struct{}

func (n *NullDb) Init() error                                         { return nil }
func (n *NullDb) Begin() error                                        { return nil }
func (n *NullDb) End() error                                          { return nil }
func (n *NullDb) Close() error                                        { return nil }
func (n *NullDb) Abort() error                                        { return nil }
func (n *NullDb) InsertPoint(element.OSMElem, interface{})            {}
func (n *NullDb) InsertLineString(element.OSMElem, interface{})       {}
func (n *NullDb) InsertPolygon(element.OSMElem, interface{})          {}
func (n *NullDb) ProbePoint(element.OSMElem) (bool, interface{})      { return true, nil }
func (n *NullDb) ProbeLineString(element.OSMElem) (bool, interface{}) { return true, nil }
func (n *NullDb) ProbePolygon(element.OSMElem) (bool, interface{})    { return true, nil }

func NewNullDb(conf Config, m *mapping.Mapping) (DB, error) {
	return &NullDb{}, nil
}

func init() {
	Register("null", NewNullDb)
}
