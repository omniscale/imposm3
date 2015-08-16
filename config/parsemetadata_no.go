// +build !parsemetadata

package config

//
// Configuration file for parsing OSM metadata
//

// for compatibility - not change this parameters!!!
// and for documentation see : parsemetadata_yes.go

const ParseMetadataVarVersion = false
const ParseMetadataVarTimestamp = false
const ParseMetadataVarChangeset = false
const ParseMetadataVarUid = false
const ParseMetadataVarUser = false

const ParseMetadataKeynameVersion = "osm_version"
const ParseMetadataKeynameTimestamp = "osm_timestamp"
const ParseMetadataKeynameChangeset = "osm_changeset"
const ParseMetadataKeynameUid = "osm_uid"
const ParseMetadataKeynameUser = "osm_user"

const ParseMetadata = false

// don't add nodes with only "created_by" tag to nodes cache  = TRUE
const ParseDontAddOnlyCreatedByTag = true
