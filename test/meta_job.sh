#!/bin/bash
set -ex


# calling:
#  ./meta_job.sh parsematadata
#  ./meta_job.sh 
#
##### environment variables
#
# GOPATH=/gopath
#
##### install osmconvert : http://wiki.openstreetmap.org/wiki/Osmconvert
#
# cd /gopath
# wget -O - http://m.m.i24.cc/osmconvert.c | cc -x c - -lz -O3 -o osmconvert
#
##### install osm_metadata branch and  build
#
# cd /gopath/src  
# git clone  https://github.com/ImreSamu/imposm3.git -b osm_metadata --single-branch /gopath/src/github.com/omniscale/imposm3 
# cd /gopath/src/github.com/omniscale/imposm3 
# go build  .
#
#####

cd /gopath/src/github.com/omniscale/imposm3

rm imposm3
go build -tags $1  .


impconnection=postgis://osm:osm@172.17.42.1/imposm4
impdata_osm=./test/meta_single_table.osm
impdata_osc=./test/meta_single_table.osc

impdata_osm_pbf=$impdata_osm.pbf
impdata_osc_gz=$impdata_osc.gz

/gopath/osmconvert $impdata_osm -o=$impdata_osm_pbf
cat $impdata_osc | gzip > $impdata_osc_gz


#####  read osm file
./imposm3 import  -mapping ./test/meta_single_table_mapping.json -read $impdata_osm_pbf -diff -write  -optimize  -overwritecache -deployproduction  -connection $impconnection
./imposm3 query-cache -node=31101
./imposm3 query-cache -way=31101
./imposm3 query-cache -rel=31101
PGPASSWORD=osm psql -U osm  -d imposm4 -h 172.17.42.1  -c 'select id,osm_id,osm_changeset,osm_version,osm_user,osm_uid,osm_timestamp,tags from public.osm_meta_all;' 


#####  read diff file
./imposm3 diff   -connection $impconnection -mapping ./test/meta_single_table_mapping.json $impdata_osc_gz
./imposm3 query-cache -node=31101
./imposm3 query-cache -way=31101
./imposm3 query-cache -rel=31101
PGPASSWORD=osm psql -U osm  -d imposm4 -h 172.17.42.1  -c 'select id,osm_id,osm_changeset,osm_version,osm_user,osm_uid,osm_timestamp,tags from public.osm_meta_all;'



#####  CLUSTER after 1 month DIFF   
PGPASSWORD=osm psql -U osm  -d imposm4 -h 172.17.42.1  -c 'CLUSTER  osm_meta_all ;'
PGPASSWORD=osm psql -U osm  -d imposm4 -h 172.17.42.1  -c 'ANALYZE  osm_meta_all ;'

