package rocksdb

type RocksDBConfig struct {
	Compression                    int  `toml:"compression"`
	BlockSize                      int  `toml:"block_size"`
	WriteBufferSize                int  `toml:"write_buffer_size"`
	CacheSize                      int  `toml:"cache_size"`
	MaxOpenFiles                   int  `toml:"max_open_files"`
	MaxWriteBufferNum              int  `toml:"max_write_buffer_num"`
	MinWriteBufferNumberToMerge    int  `toml:"min_write_buffer_number_to_merge"`
	NumLevels                      int  `toml:"num_levels"`
	Level0FileNumCompactionTrigger int  `toml:"level0_file_num_compaction_trigger"`
	Level0SlowdownWritesTrigger    int  `toml:"level0_slowdown_writes_trigger"`
	Level0StopWritesTrigger        int  `toml:"level0_stop_writes_trigger"`
	TargetFileSizeBase             int  `toml:"target_file_size_base"`
	TargetFileSizeMultiplier       int  `toml:"target_file_size_multiplier"`
	MaxBytesForLevelBase           int  `toml:"max_bytes_for_level_base"`
	MaxBytesForLevelMultiplier     int  `toml:"max_bytes_for_level_multiplier"`
	DisableAutoCompactions         bool `toml:"disable_auto_compactions"`
	DisableDataSync                bool `toml:"disable_data_sync"`
	UseFsync                       bool `toml:"use_fsync"`
	MaxBackgroundCompactions       int  `toml:"max_background_compactions"`
	MaxBackgroundFlushes           int  `toml:"max_background_flushes"`
	AllowOsBuffer                  bool `toml:"allow_os_buffer"`
	EnableStatistics               bool `toml:"enable_statistics"`
	StatsDumpPeriodSec             int  `toml:"stats_dump_period_sec"`
	BackgroundThreads              int  `toml:"background_theads"`
	HighPriorityBackgroundThreads  int  `toml:"high_priority_background_threads"`
	DisableWAL                     bool `toml:"disable_wal"`
}

func getDefault(d int, s int) int {
	if s <= 0 {
		return d
	} else {
		return s
	}
}

const (
	KB = 1024
	MB = 1024 * 1024
)

func (cfg *RocksDBConfig) Adjust() {
	cfg.CacheSize = getDefault(4*MB, cfg.CacheSize)
	cfg.BlockSize = getDefault(4*KB, cfg.BlockSize)
	cfg.WriteBufferSize = getDefault(4*MB, cfg.WriteBufferSize)
	cfg.MaxOpenFiles = getDefault(1024, cfg.MaxOpenFiles)
	cfg.MaxWriteBufferNum = getDefault(2, cfg.MaxWriteBufferNum)
	cfg.MinWriteBufferNumberToMerge = getDefault(1, cfg.MinWriteBufferNumberToMerge)
	cfg.NumLevels = getDefault(7, cfg.NumLevels)
	cfg.Level0FileNumCompactionTrigger = getDefault(4, cfg.Level0FileNumCompactionTrigger)
	cfg.Level0SlowdownWritesTrigger = getDefault(16, cfg.Level0SlowdownWritesTrigger)
	cfg.Level0StopWritesTrigger = getDefault(64, cfg.Level0StopWritesTrigger)
	cfg.TargetFileSizeBase = getDefault(32*MB, cfg.TargetFileSizeBase)
	cfg.TargetFileSizeMultiplier = getDefault(1, cfg.TargetFileSizeMultiplier)
	cfg.MaxBytesForLevelBase = getDefault(32*MB, cfg.MaxBytesForLevelBase)
	cfg.MaxBytesForLevelMultiplier = getDefault(1, cfg.MaxBytesForLevelMultiplier)
	cfg.MaxBackgroundCompactions = getDefault(1, cfg.MaxBackgroundCompactions)
	cfg.MaxBackgroundFlushes = getDefault(1, cfg.MaxBackgroundFlushes)
	cfg.StatsDumpPeriodSec = getDefault(3600, cfg.StatsDumpPeriodSec)
	cfg.BackgroundThreads = getDefault(2, cfg.BackgroundThreads)
	cfg.HighPriorityBackgroundThreads = getDefault(1, cfg.HighPriorityBackgroundThreads)
}

func DefaultConfig() *RocksDBConfig {
	cfg := &RocksDBConfig{}
	cfg.Adjust()
	return cfg
}
