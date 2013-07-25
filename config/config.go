package config

import (
	"encoding/json"
	"errors"
	"flag"
	"log"
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

var ImportFlags = flag.NewFlagSet("import", flag.ContinueOnError)
var DiffImportFlags = flag.NewFlagSet("diff", flag.ContinueOnError)

type ImportBaseOptions struct {
	Connection  string
	Cachedir    string
	MappingFile string
	Srid        int
	LimitTo     string
	ConfigFile  string
}

type _ImportOptions struct {
	Base             ImportBaseOptions
	Cpuprofile       string
	Httpprofile      string
	Memprofile       string
	Overwritecache   bool
	Appendcache      bool
	Read             string
	Write            bool
	Optimize         bool
	Diff             bool
	DeployProduction bool
	RevertDeploy     bool
	RemoveBackup     bool
	Quiet            bool
}

type _DiffImportOptions struct {
	Base ImportBaseOptions
}

var ImportOptions = _ImportOptions{}
var DiffImportOptions = _DiffImportOptions{}

func addBaseFlags(flags *flag.FlagSet, baseOptions *ImportBaseOptions) {
	flags.StringVar(&baseOptions.Connection, "connection", "", "connection parameters")
	flags.StringVar(&baseOptions.Cachedir, "cachedir", defaultCacheDir, "cache directory")
	flags.StringVar(&baseOptions.MappingFile, "mapping", "", "mapping file")
	flags.IntVar(&baseOptions.Srid, "srid", defaultSrid, "srs id")
	flags.StringVar(&baseOptions.LimitTo, "limitto", "", "limit to geometries")
	flags.StringVar(&baseOptions.ConfigFile, "config", "", "config (json)")
}

func addImportFlags(flags *flag.FlagSet, options *_ImportOptions) {
	flags.StringVar(&options.Cpuprofile, "cpuprofile", "", "filename of cpu profile output")
	flags.StringVar(&options.Httpprofile, "httpprofile", "", "bind address for profile server")
	flags.StringVar(&options.Memprofile, "memprofile", "", "dir name of mem profile output and interval (fname:interval)")
	flags.BoolVar(&options.Overwritecache, "overwritecache", false, "overwritecache")
	flags.BoolVar(&options.Appendcache, "appendcache", false, "append cache")
	flags.StringVar(&options.Read, "read", "", "read")
	flags.BoolVar(&options.Write, "write", false, "write")
	flags.BoolVar(&options.Optimize, "optimize", false, "optimize")
	flags.BoolVar(&options.Diff, "diff", false, "enable diff support")
	flags.BoolVar(&options.DeployProduction, "deployproduction", false, "deploy production")
	flags.BoolVar(&options.RevertDeploy, "revertdeploy", false, "revert deploy to production")
	flags.BoolVar(&options.RemoveBackup, "removebackup", false, "remove backups from deploy")
	flags.BoolVar(&options.Quiet, "quiet", false, "quiet log output")
}

func addDiffImportFlags(flags *flag.FlagSet, options *_DiffImportOptions) {
	// no options yet
}

func init() {
	addBaseFlags(ImportFlags, &ImportOptions.Base)
	addImportFlags(ImportFlags, &ImportOptions)
	addBaseFlags(DiffImportFlags, &DiffImportOptions.Base)
	addDiffImportFlags(DiffImportFlags, &DiffImportOptions)
}

// var (
// 	connection  = flag.String("connection", "", "connection parameters")
// 	cachedir    = flag.String("cachedir", defaultCacheDir, "cache directory")
// 	mappingFile = flag.String("mapping", "", "mapping file")
// 	srid        = flag.Int("srid", defaultSrid, "srs id")
// 	limitTo     = flag.String("limitto", "", "limit to geometries")
// 	configFile  = flag.String("config", "", "config (json)")
// )

func Parse(args []string) (*Config, []error) {
	err := ImportFlags.Parse(args)
	if err != nil {
		log.Fatal(err)
	}

	config := &Config{
		CacheDir: defaultCacheDir,
		Srid:     defaultSrid,
	}
	if ImportOptions.Base.ConfigFile != "" {
		f, err := os.Open(ImportOptions.Base.ConfigFile)
		if err != nil {
			return nil, []error{err}
		}
		decoder := json.NewDecoder(f)

		err = decoder.Decode(&config)
		if err != nil {
			return nil, []error{err}
		}
	}
	if ImportOptions.Base.Connection != "" {
		config.Connection = ImportOptions.Base.Connection
	}
	if config.Srid == 0 {
		config.Srid = defaultSrid
	}
	if ImportOptions.Base.Srid != defaultSrid {
		config.Srid = ImportOptions.Base.Srid
	}
	if ImportOptions.Base.MappingFile != "" {
		config.MappingFile = ImportOptions.Base.MappingFile
	}
	if ImportOptions.Base.LimitTo != "" {
		config.LimitTo = ImportOptions.Base.LimitTo
	}
	if ImportOptions.Base.Cachedir != defaultCacheDir {
		config.CacheDir = ImportOptions.Base.Cachedir
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
