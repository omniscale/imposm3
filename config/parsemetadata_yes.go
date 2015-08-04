// +build parsemetadata

package config



// Add special parsed meta tags to key list ( version , user, ... )

const ParseMetadataPrefix = "osm_"

const ParseMetadataVarVersion = true
const ParseMetadataVarTimestamp = true
const ParseMetadataVarChangeset = true
const ParseMetadataVarUid = true
const ParseMetadataVarUser = true

// if any ParseMetadaVar* is 'true' ->  set ParseMetadata = true   ( compile time optimalisation )
const ParseMetadata = ParseMetadataVarVersion || ParseMetadataVarTimestamp || ParseMetadataVarChangeset || ParseMetadataVarUid  || ParseMetadataVarUser 
