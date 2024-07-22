package config

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/omniscale/imposm3/log"
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
	CommitLatest        bool            `json:"commit_latest"`
	ReplicationURL      string          `json:"replication_url"`
	ReplicationInterval MinutesInterval `json:"replication_interval"`
	DiffStateBefore     MinutesInterval `json:"diff_state_before"`
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

type Base struct {
	Connection          string
	CacheDir            string
	DiffDir             string
	MappingFile         string
	Srid                int
	LimitTo             string
	LimitToCacheBuffer  float64
	ConfigFile          string
	HTTPProfile         string
	Quiet               bool
	Schemas             Schemas
	ExpireTilesDir      string
	ExpireTilesZoom     int
	CommitLatest        bool
	ReplicationURL      string
	ReplicationInterval time.Duration
	DiffStateBefore     time.Duration
	ForceDiffImport     bool
}

func (o *Base) updateFromConfig() error {
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

	if !o.CommitLatest {
		o.CommitLatest = conf.CommitLatest
	}

	if conf.ReplicationInterval.Duration != 0 && o.ReplicationInterval == time.Minute {
		o.ReplicationInterval = conf.ReplicationInterval.Duration
	}
	if o.ReplicationInterval < time.Minute {
		o.ReplicationInterval = time.Minute
	}
	o.ReplicationURL = conf.ReplicationURL

	if o.DiffDir == "" {
		if conf.DiffDir == "" {
			// use CacheDir for backwards compatibility
			o.DiffDir = o.CacheDir
		} else {
			o.DiffDir = conf.DiffDir
		}
	}

	if conf.DiffStateBefore.Duration != 0 && o.DiffStateBefore == 0 {
		o.DiffStateBefore = conf.DiffStateBefore.Duration
	}
	return nil
}

func (o *Base) check() []error {
	errs := []error{}
	if o.Srid != 3857 && o.Srid != 4326 {
		errs = append(errs, errors.New("only -srid=3857 or -srid=4326 are supported"))
	}
	if o.MappingFile == "" {
		errs = append(errs, errors.New("missing mapping"))
	}
	return errs
}

type Import struct {
	Base             Base
	Overwritecache   bool
	Appendcache      bool
	Read             string
	Write            bool
	Optimize         bool
	Diff             bool
	DeployProduction bool
	RevertDeploy     bool
	RemoveBackup     bool
}

func addBaseFlags(opts *Base, flags *flag.FlagSet) {
	flags.StringVar(&opts.Connection, "connection", "", "connection parameters")
	flags.StringVar(&opts.CacheDir, "cachedir", defaultCacheDir, "cache directory")
	flags.StringVar(&opts.DiffDir, "diffdir", "", "diff directory for last.state.txt")
	flags.StringVar(&opts.MappingFile, "mapping", "", "mapping file")
	flags.IntVar(&opts.Srid, "srid", defaultSrid, "srs id")
	flags.StringVar(&opts.LimitTo, "limitto", "", "limit to geometries")
	flags.Float64Var(&opts.LimitToCacheBuffer, "limittocachebuffer", 0.0, "limit to buffer for cache")
	flags.StringVar(&opts.ConfigFile, "config", "", "config (json)")
	flags.StringVar(&opts.HTTPProfile, "httpprofile", "", "bind address for profile server")
	flags.BoolVar(&opts.Quiet, "quiet", false, "quiet log output")
	flags.StringVar(&opts.Schemas.Import, "dbschema-import", defaultSchemaImport, "db schema for imports")
	flags.StringVar(&opts.Schemas.Production, "dbschema-production", defaultSchemaProduction, "db schema for production")
	flags.StringVar(&opts.Schemas.Backup, "dbschema-backup", defaultSchemaBackup, "db schema for backups")
}

func isFlagActual(flags *flag.FlagSet, name string) bool {
	found := false
	flags.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func ParseImport(args []string) Import {
	flags := flag.NewFlagSet("import", flag.ExitOnError)
	opts := Import{}

	addBaseFlags(&opts.Base, flags)
	flags.BoolVar(&opts.Overwritecache, "overwritecache", false, "overwritecache")
	flags.BoolVar(&opts.Appendcache, "appendcache", false, "append cache")
	flags.StringVar(&opts.Read, "read", "", "read")
	flags.BoolVar(&opts.Write, "write", false, "write")
	flags.BoolVar(&opts.Optimize, "optimize", false, "optimize")
	flags.BoolVar(&opts.Diff, "diff", false, "enable diff support")
	flags.BoolVar(&opts.DeployProduction, "deployproduction", false, "deploy production")
	flags.BoolVar(&opts.RevertDeploy, "revertdeploy", false, "revert deploy to production")
	flags.BoolVar(&opts.RemoveBackup, "removebackup", false, "remove backups from deploy")
	flags.DurationVar(&opts.Base.DiffStateBefore, "diff-state-before", 0, "set initial diff sequence before")
	flags.DurationVar(&opts.Base.ReplicationInterval, "replication-interval", time.Minute, "replication interval as duration (1m, 1h, 24h)")

	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s %s [args]\n\n", os.Args[0], os.Args[1])
		flags.PrintDefaults()
		os.Exit(2)
	}

	if len(args) == 0 {
		flags.Usage()
	}

	err := flags.Parse(args)
	if err != nil {
		log.Fatal(err)
	}
	err = opts.Base.updateFromConfig()
	if err != nil {
		log.Fatal(err)
	}
	errs := opts.Base.check()
	if len(errs) != 0 {
		reportErrors(errs)
		flags.Usage()
	}
	return opts
}

func ParseDiffImport(args []string) (Base, []string) {
	flags := flag.NewFlagSet("diff", flag.ExitOnError)
	opts := Base{}

	addBaseFlags(&opts, flags)
	flags.StringVar(&opts.ExpireTilesDir, "expiretiles-dir", "", "write expire tiles into dir")
	flags.IntVar(&opts.ExpireTilesZoom, "expiretiles-zoom", 14, "write expire tiles in this zoom level")
	flags.BoolVar(&opts.ForceDiffImport, "force", false, "force import of diff if sequence was already imported")
	flags.BoolVar(&opts.CommitLatest, "commit-latest", false, "commit after last diff, instead after each diff")

	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s %s [args] [.osc.gz, ...]\n\n", os.Args[0], os.Args[1])
		flags.PrintDefaults()
		os.Exit(2)
	}

	if len(args) == 0 {
		flags.Usage()
	}

	err := flags.Parse(args)
	if err != nil {
		log.Fatal(err)
	}

	// If expiretiles-zoom hasn't been explicitly set on the command-line, use the
	// value from config.json or fall back to the default value.
	if !isFlagActual(flags, "expiretiles-zoom") {
		flags.Set("expiretiles-zoom", "0")
	}
	err = opts.updateFromConfig()
	if err != nil {
		log.Fatal(err)
	}

	errs := opts.check()
	if len(errs) != 0 {
		reportErrors(errs)
		flags.Usage()
	}

	return opts, flags.Args()
}

func ParseRunImport(args []string) Base {
	flags := flag.NewFlagSet("run", flag.ExitOnError)
	opts := Base{}

	addBaseFlags(&opts, flags)
	flags.StringVar(&opts.ExpireTilesDir, "expiretiles-dir", "", "write expire tiles into dir")
	flags.IntVar(&opts.ExpireTilesZoom, "expiretiles-zoom", 14, "write expire tiles in this zoom level")
	flags.BoolVar(&opts.CommitLatest, "commit-latest", false, "commit after last diff, instead after each diff")
	flags.DurationVar(&opts.ReplicationInterval, "replication-interval", time.Minute, "replication interval as duration (1m, 1h, 24h)")

	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s %s [args]\n\n", os.Args[0], os.Args[1])
		flags.PrintDefaults()
		os.Exit(2)
	}

	if len(args) == 0 {
		flags.Usage()
	}

	err := flags.Parse(args)
	if err != nil {
		log.Fatal(err)
	}

	// If expiretiles-zoom hasn't been explicitly set on the command-line, use the
	// value from config.json or fall back to the default value.
	if !isFlagActual(flags, "expiretiles-zoom") {
		flags.Set("expiretiles-zoom", "0")
	}
	err = opts.updateFromConfig()
	if err != nil {
		log.Fatal(err)
	}

	errs := opts.check()
	if len(errs) != 0 {
		reportErrors(errs)
		flags.Usage()
	}

	return opts
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
