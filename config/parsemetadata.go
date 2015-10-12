package config

//
// Configuration file for parsing OSM metadata, updated via mapping configuration
//

// individual metadata parsing
var ParseMetadataVarVersion = false
var ParseMetadataVarTimestamp = false
var ParseMetadataVarChangeset = false
var ParseMetadataVarUid = false
var ParseMetadataVarUser = false

// if any ParseMetadaVar* is 'true' ->  set ParseMetadata = true
var ParseMetadata = ParseMetadataVarVersion || ParseMetadataVarTimestamp || ParseMetadataVarChangeset || ParseMetadataVarUid || ParseMetadataVarUser

// For compatibiliy with other osm tools (GDAL,OSM2PGSQL) the default metadat keymames are osm_*
var ParseMetadataKeynameVersion = "osm_version"
var ParseMetadataKeynameTimestamp = "osm_timestamp"
var ParseMetadataKeynameChangeset = "osm_changeset"
var ParseMetadataKeynameUid = "osm_uid"
var ParseMetadataKeynameUser = "osm_user"

// don't add nodes/ways/relations with only "created_by" tag to nodes cache  = FALSE
var ParseDontAddOnlyCreatedByTag = true
