package cache

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
)

type cacheOptions struct {
	CacheSizeM           int
	MaxOpenFiles         int
	BlockRestartInterval int
	WriteBufferSizeM     int
	BlockSizeK           int
}

type coordsCacheOptions struct {
	cacheOptions
	BunchSize          int
	BunchCacheCapacity int
}
type osmCacheOptions struct {
	Coords       coordsCacheOptions
	Ways         cacheOptions
	Nodes        cacheOptions
	Relations    cacheOptions
	InsertedWays cacheOptions
	CoordsIndex  cacheOptions
	WaysIndex    cacheOptions
}

const defaultConfig = `
{
    "Coords": {
        "CacheSizeM": 16,
        "WriteBufferSizeM": 64,
        "BlockSizeK": 0,
        "MaxOpenFiles": 64,
        "BlockRestartInterval": 256,
        "BunchSize": 32,
        "BunchCacheCapacity": 8096
    },
    "Nodes": {
        "CacheSizeM": 16,
        "WriteBufferSizeM": 64,
        "BlockSizeK": 0,
        "MaxOpenFiles": 64,
        "BlockRestartInterval": 128
    },
    "Ways": {
        "CacheSizeM": 16,
        "WriteBufferSizeM": 64,
        "BlockSizeK": 0,
        "MaxOpenFiles": 64,
        "BlockRestartInterval": 128
    },
    "Relations": {
        "CacheSizeM": 16,
        "WriteBufferSizeM": 64,
        "BlockSizeK": 0,
        "MaxOpenFiles": 64,
        "BlockRestartInterval": 128
    },
    "InsertedWays": {
        "CacheSizeM": 0,
        "WriteBufferSizeM": 0,
        "BlockSizeK": 0,
        "MaxOpenFiles": 0,
        "BlockRestartInterval": 0
    },
    "CoordsIndex": {
        "CacheSizeM": 32,
        "WriteBufferSizeM": 128,
        "BlockSizeK": 0,
        "MaxOpenFiles": 256,
        "BlockRestartInterval": 256
    },
    "WaysIndex": {
        "CacheSizeM": 16,
        "WriteBufferSizeM": 64,
        "BlockSizeK": 0,
        "MaxOpenFiles": 64,
        "BlockRestartInterval": 128
    }
}
`

var globalCacheOptions osmCacheOptions

func init() {
	err := json.Unmarshal([]byte(defaultConfig), &globalCacheOptions)
	if err != nil {
		panic(err)
	}

	cacheConfFile := os.Getenv("GOPOSM_CACHE_CONFIG")
	if cacheConfFile != "" {
		data, err := ioutil.ReadFile(cacheConfFile)
		if err != nil {
			log.Println("Unable to read cache config:", err)
		}
		err = json.Unmarshal(data, &globalCacheOptions)
		if err != nil {
			log.Println("Unable to parse cache config:", err)
		}
	}
}
