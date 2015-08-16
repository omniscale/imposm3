// +build parsemetadata

package config


//
// Configuration file for parsing OSM metadata 
//


// if you not need all osm metadata - set any flag to 'false'
const ParseMetadataVarVersion = true
const ParseMetadataVarTimestamp = true
const ParseMetadataVarChangeset = true
const ParseMetadataVarUid = true
const ParseMetadataVarUser = true

//  
// For compatibiliy with other osm tools (GDAL,OSM2PGSQL) the default metadat keymames are osm_*  
//   
// Be carefull before rename ! 
// and check taginfo :)
// - key:"changeset" - http://taginfo.openstreetmap.org/keys/changeset ( count : 19 )
// - key:"version" - http://taginfo.openstreetmap.org/keys/version ( count : 3448 )
// - key:"user" - http://taginfo.openstreetmap.org/keys/user ( count : 197 )

const ParseMetadataKeynameVersion = "osm_version"
const ParseMetadataKeynameTimestamp = "osm_timestamp"
const ParseMetadataKeynameChangeset = "osm_changeset"
const ParseMetadataKeynameUid = "osm_uid"
const ParseMetadataKeynameUser = "osm_user"


// ----------   alternative keynames for custom build
// const ParseMetadataKeynameVersion   = "_version_"
// const ParseMetadataKeynameTimestamp = "_timestamp_"
// const ParseMetadataKeynameChangeset = "_changeset_"
// const ParseMetadataKeynameUid       = "_uid_"
// const ParseMetadataKeynameUser      = "_user_"


// if any ParseMetadaVar* is 'true' ->  set ParseMetadata = true   ( code optimalisation )
const ParseMetadata = ParseMetadataVarVersion || ParseMetadataVarTimestamp || ParseMetadataVarChangeset || ParseMetadataVarUid  || ParseMetadataVarUser 
