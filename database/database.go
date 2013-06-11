package database

import (
	"goposm/mapping"
	"strings"
)

type Config struct {
	Type             string
	ConnectionParams string
	Srid             int
}

type DB interface {
	Init() error
	InsertBatch(string, [][]interface{}) error
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
		panic("unsupported database type: " + conf.Type)
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

func (n *NullDb) Init() error                               { return nil }
func (n *NullDb) InsertBatch(string, [][]interface{}) error { return nil }

func NewNullDb(conf Config, m *mapping.Mapping) (DB, error) {
	return &NullDb{}, nil
}

func init() {
	Register("null", NewNullDb)
}
