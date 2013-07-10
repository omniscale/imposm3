package config

import (
	"encoding/json"
	"errors"
	"flag"
	"os"
)

type Config struct {
	CacheDir    string `json:"cachedir"`
	Connection  string `json:"connection"`
	MappingFile string `json:"mapping"`
	LimitTo     string `json:"limitto"`
	Srid        int    `json:"srid"`
}

const defaultSrid = 3857
const defaultCacheDir = "/tmp/goposm"

var (
	connection  = flag.String("connection", "", "connection parameters")
	cachedir    = flag.String("cachedir", defaultCacheDir, "cache directory")
	mappingFile = flag.String("mapping", "", "mapping file")
	srid        = flag.Int("srid", defaultSrid, "srs id")
	limitTo     = flag.String("limitto", "", "limit to geometries")
	configFile  = flag.String("config", "", "config (json)")
)

func Parse() (*Config, []error) {
	config := &Config{
		CacheDir: defaultCacheDir,
		Srid:     defaultSrid,
	}
	if *configFile != "" {
		f, err := os.Open(*configFile)
		if err != nil {
			return nil, []error{err}
		}
		decoder := json.NewDecoder(f)

		err = decoder.Decode(&config)
		if err != nil {
			return nil, []error{err}
		}
	}
	if *connection != "" {
		config.Connection = *connection
	}
	if config.Srid == 0 {
		config.Srid = defaultSrid
	}
	if *srid != defaultSrid {
		config.Srid = *srid
	}
	if *mappingFile != "" {
		config.MappingFile = *mappingFile
	}
	if *limitTo != "" {
		config.LimitTo = *limitTo
	}
	if *cachedir != defaultCacheDir {
		config.CacheDir = *cachedir
	}

	errs := checkConfig(config)
	return config, errs
}

func checkConfig(config *Config) []error {
	errs := []error{}
	if config.Srid != 3857 {
		errs = append(errs, errors.New("srid!=3857 not implemented"))
	}
	if config.MappingFile == "" {
		errs = append(errs, errors.New("missing mapping"))
	}
	if config.Connection == "" {
		errs = append(errs, errors.New("missing connection"))
	}
	return errs
}
