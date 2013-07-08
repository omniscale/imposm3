package config

import (
	"encoding/json"
	"errors"
	"flag"
	"os"
)

type Config struct {
	Cachedir    string `json:"cachedir"`
	Connection  string `json:"connection"`
	MappingFile string `json:"mapping"`
	LimitTo     string `json:"limitto"`
	Srid        int    `json:"srid"`
}

var (
	connection  = flag.String("connection", "", "connection parameters")
	mappingFile = flag.String("mapping", "", "mapping file")
	srid        = flag.Int("srid", 3857, "srs id")
	configFile  = flag.String("config", "", "config (json)")
)

func Parse() (*Config, []error) {
	config := &Config{}
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
		config.Srid = 3857
	}
	if *srid != 3857 {
		config.Srid = *srid
	}
	if *mappingFile != "" {
		config.MappingFile = *mappingFile
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

// cpuprofile       = flag.String("cpuprofile", "", "filename of cpu profile output")
// httpprofile      = flag.String("httpprofile", "", "bind address for profile server")
// memprofile       = flag.String("memprofile", "", "dir name of mem profile output and interval (fname:interval)")
// cachedir         = flag.String("cachedir", "/tmp/goposm", "cache directory")
// overwritecache   = flag.Bool("overwritecache", false, "overwritecache")
// appendcache      = flag.Bool("appendcache", false, "append cache")
// read             = flag.String("read", "", "read")
// write            = flag.Bool("write", false, "write")
// optimize         = flag.Bool("optimize", false, "optimize")
// diff             = flag.Bool("diff", false, "enable diff support")
// mappingFile      = flag.String("mapping", "", "mapping file")
// deployProduction = flag.Bool("deployproduction", false, "deploy production")
// revertDeploy     = flag.Bool("revertdeploy", false, "revert deploy to production")
// removeBackup     = flag.Bool("removebackup", false, "remove backups from deploy")
// limitTo          = flag.String("limitto", "", "limit to geometries")
// quiet            = flag.Bool("quiet", false, "quiet log output")
