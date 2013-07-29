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

var ImportFlags = flag.NewFlagSet("import", flag.ExitOnError)
var DiffImportFlags = flag.NewFlagSet("diff", flag.ExitOnError)

type ImportBaseOptions struct {
	Connection  string
	CacheDir    string
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
	flags.StringVar(&baseOptions.CacheDir, "cachedir", defaultCacheDir, "cache directory")
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

func ParseImport(args []string) []error {
	err := ImportFlags.Parse(args)
	if err != nil {
		log.Fatal(err)
	}
	errs := updateBaseOpts(&ImportOptions.Base)
	if errs != nil {
		return errs
	}

	errs = checkOptions(&ImportOptions.Base)
	return errs
}

func updateBaseOpts(opts *ImportBaseOptions) []error {

	conf := &Config{
		CacheDir: defaultCacheDir,
		Srid:     defaultSrid,
	}

	if opts.ConfigFile != "" {
		f, err := os.Open(opts.ConfigFile)
		if err != nil {
			return []error{err}
		}
		decoder := json.NewDecoder(f)

		err = decoder.Decode(&conf)
		if err != nil {
			return []error{err}
		}
	}
	if opts.Connection == "" {
		opts.Connection = conf.Connection
	}
	if conf.Srid == 0 {
		conf.Srid = defaultSrid
	}
	if opts.Srid != defaultSrid {
		opts.Srid = conf.Srid
	}
	if opts.MappingFile == "" {
		opts.MappingFile = conf.MappingFile
	}
	if opts.LimitTo == "" {
		opts.LimitTo = conf.LimitTo
	}
	if opts.CacheDir == defaultCacheDir {
		opts.CacheDir = conf.CacheDir
	}
	return nil
}

func ParseDiffImport(args []string) []error {
	err := DiffImportFlags.Parse(args)
	if err != nil {
		log.Fatal(err)
	}

	errs := updateBaseOpts(&DiffImportOptions.Base)
	if errs != nil {
		return errs
	}

	errs = checkOptions(&DiffImportOptions.Base)

	return errs
}

func checkOptions(opts *ImportBaseOptions) []error {
	errs := []error{}
	if opts.Srid != 3857 {
		errs = append(errs, errors.New("srid!=3857 not implemented"))
	}
	if opts.MappingFile == "" {
		errs = append(errs, errors.New("missing mapping"))
	}
	if opts.Connection == "" {
		errs = append(errs, errors.New("missing connection"))
	}
	return errs
}
