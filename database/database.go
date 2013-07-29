package database

import (
	"errors"
	"goposm/mapping"
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
	RowInserter
}

type BulkBeginner interface {
	BeginBulk() error
}

type RowInserter interface {
	Insert(string, []interface{})
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

func (n *NullDb) Init() error                  { return nil }
func (n *NullDb) Begin() error                 { return nil }
func (n *NullDb) End() error                   { return nil }
func (n *NullDb) Close() error                 { return nil }
func (n *NullDb) Abort() error                 { return nil }
func (n *NullDb) Insert(string, []interface{}) {}

func NewNullDb(conf Config, m *mapping.Mapping) (DB, error) {
	return &NullDb{}, nil
}

func init() {
	Register("null", NewNullDb)
}
