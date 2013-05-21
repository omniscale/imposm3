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
	Init(*mapping.Mapping) error
	InsertBatch(string, [][]interface{}) error
}

var databases map[string]func(Config) (DB, error)

func Register(name string, f func(Config) (DB, error)) {
	if databases == nil {
		databases = make(map[string]func(Config) (DB, error))
	}
	databases[name] = f
}

func Open(conf Config) (DB, error) {
	newFunc, ok := databases[conf.Type]
	if !ok {
		panic("unsupported database type: " + conf.Type)
	}

	db, err := newFunc(conf)
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

func (n *NullDb) Init(*mapping.Mapping) error               { return nil }
func (n *NullDb) InsertBatch(string, [][]interface{}) error { return nil }

func NewNullDb(conf Config) (DB, error) {
	return &NullDb{}, nil
}

func init() {
	Register("null", NewNullDb)
}
