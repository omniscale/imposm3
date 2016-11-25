package config

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

type Config struct {
	CacheDir            string          `json:"cachedir"`
	DiffDir             string          `json:"diffdir"`
	Connection          string          `json:"connection"`
	MappingFile         string          `json:"mapping"`
	LimitTo             string          `json:"limitto"`
	LimitToCacheBuffer  float64         `json:"limitto_cache_buffer"`
	Srid                int             `json:"srid"`
	Schemas             Schemas         `json:"schemas"`
	ExpireTilesDir      string          `json:"expiretiles_dir"`
	ExpireTilesZoom     int             `json:"expiretiles_zoom"`
	ReplicationUrl      string          `json:"replication_url"`
	ReplicationInterval MinutesInterval `json:"replication_interval"`
}

type Schemas struct {
	Import     string `json:"import"`
	Production string `json:"production"`
	Backup     string `json:"backup"`
}

const defaultSrid = 3857
const defaultCacheDir = "/tmp/imposm3"
const defaultSchemaImport = "import"
const defaultSchemaProduction = "public"
const defaultSchemaBackup = "backup"

var ImportFlags = flag.NewFlagSet("import", flag.ExitOnError)
var DiffFlags = flag.NewFlagSet("diff", flag.ExitOnError)
var RunFlags = flag.NewFlagSet("run", flag.ExitOnError)

type _BaseOptions struct {
	Connection          string
	CacheDir            string
	DiffDir             string
	MappingFile         string
	Srid                int
	LimitTo             string
	LimitToCacheBuffer  float64
	ConfigFile          string
	Httpprofile         string
	Quiet               bool
	Schemas             Schemas
	ExpireTilesDir      string
	ExpireTilesZoom     int
	ReplicationUrl      string
	ReplicationInterval time.Duration
}

func (o *_BaseOptions) updateFromConfig() error {
	conf := &Config{
		CacheDir: defaultCacheDir,
		Srid:     defaultSrid,
	}

	if o.ConfigFile != "" {
		f, err := os.Open(o.ConfigFile)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(f)

		err = decoder.Decode(&conf)
		if err != nil {
			return err
		}
	}

	if conf.Schemas.Import != "" && o.Schemas.Import == defaultSchemaImport {
		o.Schemas.Import = conf.Schemas.Import
	}
	if conf.Schemas.Production != "" && o.Schemas.Production == defaultSchemaProduction {
		o.Schemas.Production = conf.Schemas.Production
	}
	if conf.Schemas.Backup != "" && o.Schemas.Backup == defaultSchemaBackup {
		o.Schemas.Backup = conf.Schemas.Backup
	}

	if o.Connection == "" {
		o.Connection = conf.Connection
	}
	if conf.Srid == 0 {
		conf.Srid = defaultSrid
	}
	if o.Srid == defaultSrid {
		o.Srid = conf.Srid
	}
	if o.MappingFile == "" {
		o.MappingFile = conf.MappingFile
	}
	if o.LimitTo == "" {
		o.LimitTo = conf.LimitTo
	}
	if o.LimitTo == "NONE" {
		// allow overwrite from cmd line
		o.LimitTo = ""
	}
	if o.LimitToCacheBuffer == 0.0 {
		o.LimitToCacheBuffer = conf.LimitToCacheBuffer
	}
	if o.CacheDir == defaultCacheDir {
		o.CacheDir = conf.CacheDir
	}

	if o.ExpireTilesDir == "" {
		o.ExpireTilesDir = conf.ExpireTilesDir
	}
	if o.ExpireTilesZoom == 0 {
		o.ExpireTilesZoom = conf.ExpireTilesZoom
	}
	if o.ExpireTilesZoom < 6 || o.ExpireTilesZoom > 18 {
		o.ExpireTilesZoom = 14
	}

	if conf.ReplicationInterval.Duration != 0 && o.ReplicationInterval != time.Minute {
		o.ReplicationInterval = conf.ReplicationInterval.Duration
	}
	if o.ReplicationInterval < time.Minute {
		o.ReplicationInterval = time.Minute
	}
	o.ReplicationUrl = conf.ReplicationUrl

	if o.DiffDir == "" {
		if conf.DiffDir == "" {
			// use CacheDir for backwards compatibility
			o.DiffDir = o.CacheDir
		} else {
			o.DiffDir = conf.DiffDir
		}
	}
	return nil
}

func (o *_BaseOptions) check() []error {
	errs := []error{}
	if o.Srid != 3857 && o.Srid != 4326 {
		errs = append(errs, errors.New("only -srid=3857 or -srid=4326 are supported"))
	}
	if o.MappingFile == "" {
		errs = append(errs, errors.New("missing mapping"))
	}
	return errs
}

type _ImportOptions struct {
	Overwritecache   bool
	Appendcache      bool
	Read             string
	Write            bool
	Optimize         bool
	Diff             bool
	DeployProduction bool
	RevertDeploy     bool
	RemoveBackup     bool
	DiffStateBefore  time.Duration
}

var BaseOptions = _BaseOptions{}
var ImportOptions = _ImportOptions{}

func addBaseFlags(flags *flag.FlagSet) {
	flags.StringVar(&BaseOptions.Connection, "connection", "", "connection parameters")
	flags.StringVar(&BaseOptions.CacheDir, "cachedir", defaultCacheDir, "cache directory")
	flags.StringVar(&BaseOptions.DiffDir, "diffdir", "", "diff directory for last.state.txt")
	flags.StringVar(&BaseOptions.MappingFile, "mapping", "", "mapping file")
	flags.IntVar(&BaseOptions.Srid, "srid", defaultSrid, "srs id")
	flags.StringVar(&BaseOptions.LimitTo, "limitto", "", "limit to geometries")
	flags.Float64Var(&BaseOptions.LimitToCacheBuffer, "limittocachebuffer", 0.0, "limit to buffer for cache")
	flags.StringVar(&BaseOptions.ConfigFile, "config", "", "config (json)")
	flags.StringVar(&BaseOptions.Httpprofile, "httpprofile", "", "bind address for profile server")
	flags.BoolVar(&BaseOptions.Quiet, "quiet", false, "quiet log output")
	flags.StringVar(&BaseOptions.Schemas.Import, "dbschema-import", defaultSchemaImport, "db schema for imports")
	flags.StringVar(&BaseOptions.Schemas.Production, "dbschema-production", defaultSchemaProduction, "db schema for production")
	flags.StringVar(&BaseOptions.Schemas.Backup, "dbschema-backup", defaultSchemaBackup, "db schema for backups")
}

func UsageImport() {
	fmt.Fprintf(os.Stderr, "Usage: %s %s [args]\n\n", os.Args[0], os.Args[1])
	ImportFlags.PrintDefaults()
	os.Exit(2)
}

func UsageDiff() {
	fmt.Fprintf(os.Stderr, "Usage: %s %s [args] [.osc.gz, ...]\n\n", os.Args[0], os.Args[1])
	DiffFlags.PrintDefaults()
	os.Exit(2)
}

func UsageRun() {
	fmt.Fprintf(os.Stderr, "Usage: %s %s [args] [.osc.gz, ...]\n\n", os.Args[0], os.Args[1])
	DiffFlags.PrintDefaults()
	os.Exit(2)
}

func init() {
	ImportFlags.Usage = UsageImport
	DiffFlags.Usage = UsageDiff
	RunFlags.Usage = UsageRun

	addBaseFlags(DiffFlags)
	addBaseFlags(ImportFlags)
	addBaseFlags(RunFlags)
	ImportFlags.BoolVar(&ImportOptions.Overwritecache, "overwritecache", false, "overwritecache")
	ImportFlags.BoolVar(&ImportOptions.Appendcache, "appendcache", false, "append cache")
	ImportFlags.StringVar(&ImportOptions.Read, "read", "", "read")
	ImportFlags.BoolVar(&ImportOptions.Write, "write", false, "write")
	ImportFlags.BoolVar(&ImportOptions.Optimize, "optimize", false, "optimize")
	ImportFlags.BoolVar(&ImportOptions.Diff, "diff", false, "enable diff support")
	ImportFlags.BoolVar(&ImportOptions.DeployProduction, "deployproduction", false, "deploy production")
	ImportFlags.BoolVar(&ImportOptions.RevertDeploy, "revertdeploy", false, "revert deploy to production")
	ImportFlags.BoolVar(&ImportOptions.RemoveBackup, "removebackup", false, "remove backups from deploy")
	ImportFlags.DurationVar(&ImportOptions.DiffStateBefore, "diff-state-before", 2*time.Hour, "set initial diff sequence before")

	DiffFlags.StringVar(&BaseOptions.ExpireTilesDir, "expiretiles-dir", "", "write expire tiles into dir")
	DiffFlags.IntVar(&BaseOptions.ExpireTilesZoom, "expiretiles-zoom", 14, "write expire tiles in this zoom level")

	RunFlags.StringVar(&BaseOptions.ExpireTilesDir, "expiretiles-dir", "", "write expire tiles into dir")
	RunFlags.IntVar(&BaseOptions.ExpireTilesZoom, "expiretiles-zoom", 14, "write expire tiles in this zoom level")
	RunFlags.DurationVar(&BaseOptions.ReplicationInterval, "replication-interval", time.Minute, "replication interval as duration (1m, 1h, 24h)")
}

func ParseImport(args []string) {
	if len(args) == 0 {
		UsageImport()
	}
	err := ImportFlags.Parse(args)
	if err != nil {
		log.Fatal(err)
	}
	err = BaseOptions.updateFromConfig()
	if err != nil {
		log.Fatal(err)
	}
	errs := BaseOptions.check()
	if len(errs) != 0 {
		reportErrors(errs)
		UsageImport()
	}
}

func ParseDiffImport(args []string) {
	if len(args) == 0 {
		UsageDiff()
	}
	err := DiffFlags.Parse(args)
	if err != nil {
		log.Fatal(err)
	}

	err = BaseOptions.updateFromConfig()
	if err != nil {
		log.Fatal(err)
	}

	errs := BaseOptions.check()
	if len(errs) != 0 {
		reportErrors(errs)
		UsageDiff()
	}
}

func ParseRunImport(args []string) {
	if len(args) == 0 {
		UsageRun()
	}
	err := RunFlags.Parse(args)
	if err != nil {
		log.Fatal(err)
	}

	err = BaseOptions.updateFromConfig()
	if err != nil {
		log.Fatal(err)
	}

	errs := BaseOptions.check()
	if len(errs) != 0 {
		reportErrors(errs)
		UsageRun()
	}
}

func reportErrors(errs []error) {
	fmt.Println("errors in config/options:")
	for _, err := range errs {
		fmt.Printf("\t%s\n", err)
	}
	os.Exit(1)
}

type MinutesInterval struct {
	time.Duration
}

func (d *MinutesInterval) UnmarshalJSON(b []byte) (err error) {
	if b[0] == '"' {
		sd := string(b[1 : len(b)-1])
		d.Duration, err = time.ParseDuration(sd)
		return
	}

	var id int64
	id, err = json.Number(string(b)).Int64()
	d.Duration = time.Duration(id) * time.Minute

	return
}
