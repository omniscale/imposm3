
### Imposm3 - add osm metadata 

Implementing "osm2pgsql --extra-attributes " functionality 
Only a simple "proof of concept" ( I am still learning golang )

This patch add 5 osm_* meta key-values to the other tags:  
* osm_changeset 
* osm_version 
* osm_user 
* osm_uid 
* osm_timestamp  

example
```json
{
  "nodes": {
    "31101": {
      "tags": {
        "amenity": "cafe",
        "osm_changeset": "3000001",
        "osm_timestamp": "2011-11-11T00:11:11Z",
        "osm_uid": "301",
        "osm_user": "user301",
        "osm_version": "11"
      },
      "lat": 46.9999999342775,
      "lon": 79.99999998675656
    }
  }
}
```
Be careful, this mode
* increase the size of cache files!!!
* slow down the performance


if you want switch on this functionality, use go build tag **'parsemetadata'**, 
example:  **"go build -tags parsemetadata  . "** 

Status ( 2014-08-04 ) :
- [x] "import" mode 
- [x] "diff" mode
- [x] go build tag : **"parsemetadata"**
  - [x]  parse mode:   **"go build -tags parsemetadata  . "**
  - [x] normal mode:   **"go build .  "**
- [x] internal config parameters for easy hackability and minimal cache files ( prefix, choosing meta vars )  ./config/parsemetadata_yes.go   
  - [x] ParseMetadataPrefix = "osm_"
  - [x] ParseMetadataVarVersion   ( add : "osm_version": "11" )
  - [x] ParseMetadataVarTimestamp ( add : "osm_timestamp": "2011-11-11T00:11:11Z", )
  - [x] ParseMetadataVarChangeset ( add : "osm_changeset": "3000001", )
  - [x] ParseMetadataVarUid       ( add : "osm_uid": "301", )   
  - [x] ParseMetadataVarUser      ( add : "osm_user": "user301", )
- [ ] suggestions: external config parameters  ( prefix, choosing meta vars) 
  - [ ] --ParseMetadataPrefix = "osm_"
  - [ ] --ParseMetadataVarVersion   ( add : "osm_version": "11" )
  - [ ] --ParseMetadataVarTimestamp ( add : "osm_timestamp": "2011-11-11T00:11:11Z", )
  - [ ] --ParseMetadataVarChangeset ( add : "osm_changeset": "3000001", )
  - [ ] --ParseMetadataVarUid       ( add : "osm_uid": "301", )   
  - [ ] --ParseMetadataVarUser      ( add : "osm_user": "user301", ) 
  - [ ] or a simple:  --ParseMetadata ?  
- [ ] makefile integration
  - [ ] ?? more executables for compatibility and performance?  **"imposm3_meta"** ?
  - [ ] ?? custom builds?  
- [ ] optimal code
- [ ] minimal test data and code:
  - [x] **"./test/meta_job.sh  parsemetadata"**    for testing "parsemetadata" mode
  - [x] **"./test/meta_job.sh "**                  for testing normal mode 
  - [x] ./test/meta_single_table_mapping.json  
  - [x] ./test/meta_single_table.osm
  - [x] ./test/meta_single_table.osc  
  - [ ] ./test/meta_single_table_test.py    
- [ ] full test code ( python )
- [ ] performance testing
- [ ] documentation
- [ ] tutorial

### Preparation

my gopath
```bash
GOPATH=/gopath
```

install osmconvert : http://wiki.openstreetmap.org/wiki/Osmconvert

```bash
cd /gopath
wget -O - http://m.m.i24.cc/osmconvert.c | cc -x c - -lz -O3 -o osmconvert
```


#### install code
```bash
cd /gopath/src  

#before accepting pull request, you can test with this :
git clone  https://github.com/ImreSamu/imposm3.git -b osm_metadata --single-branch /gopath/src/github.com/omniscale/imposm3 
#after use the standard :   go get github.com/omniscale/imposm3

cd /gopath/src/github.com/omniscale/imposm3 
go build -tags parsemetadata  . 
```


### Log files ( "./test/meta_job.sh parsemetadata"  )

set parameters and convert osm,osc files to pbf and osc.gz:

```bash
cd /gopath/src/github.com/omniscale/imposm3
go build -tags parsemetadata  . 
impconnection=postgis://osm:osm@172.17.42.1/imposm4
impdata_osm=./test/meta_single_table.osm
impdata_osc=./test/meta_single_table.osc
impdata_osm_pbf=$impdata_osm.pbf
impdata_osc_gz=$impdata_osc.gz
/gopath/osmconvert $impdata_osm -o=$impdata_osm_pbf
cat $impdata_osc | gzip > $impdata_osc_gz

```

#### imposm3 import
```bash
./imposm3 import -mapping ./test/meta_single_table_mapping.json -read ./test/meta_single_table.osm.pbf -diff -write -overwritecache -deployproduction -connection postgis://osm:osm@172.17.42.1/imposm4
```

```
[Aug  3 23:14:59] [INFO] removing existing cache /tmp/imposm3
[Aug  3 23:15:00] [INFO] [     0] C:       0/s (5) N:       0/s (1) W:       0/s (3) R:      0/s (1)
[Aug  3 23:15:00] [INFO] Reading OSM data took: 320.89263ms
[Aug  3 23:15:00] [INFO] [     0] C:       0/s ( 0.0%) N:       0/s (100.0%) W:       0/s (100.0%) R:      0/s (100.0%)
[Aug  3 23:15:00] [INFO] Writing OSM data took: 608.304079ms
[Aug  3 23:15:00] [INFO] [PostGIS] Creating generalized tables took: 42.582µs
[Aug  3 23:15:00] [INFO] [PostGIS] Creating OSM id index on osm_meta_all took: 1.53095ms
[Aug  3 23:15:00] [INFO] [PostGIS] Creating geometry index on osm_meta_all took: 1.150422ms
[Aug  3 23:15:00] [INFO] [PostGIS] Creating geometry indices took: 2.926129ms
[Aug  3 23:15:00] [INFO] Importing OSM data took: 611.45983ms
[Aug  3 23:15:00] [INFO] [PostGIS] Rotating osm_meta_all from import -> public -> backup
[Aug  3 23:15:00] [INFO] [PostGIS] backup of osm_meta_all, to backup
[Aug  3 23:15:00] [INFO] [PostGIS] Rotating tables took: 14.776169ms
[Aug  3 23:15:00] [INFO] Imposm took: 1.05608454s
```

```bash
./imposm3 query-cache -node=31101
{
  "nodes": {
    "31101": {
      "tags": {
        "amenity": "cafe",
        "osm_changeset": "3000001",
        "osm_timestamp": "2011-11-11T00:11:11Z",
        "osm_uid": "301",
        "osm_user": "user301",
        "osm_version": "11"
      },
      "lat": 46.9999999342775,
      "lon": 79.99999998675656
    }
  }
}

./imposm3 query-cache -way=31101
{
  "ways": {
    "31101": {
      "tags": {
        "highway": "secondary",
        "landuse": "park",
        "osm_changeset": "3000002",
        "osm_timestamp": "2011-11-11T00:11:22Z",
        "osm_uid": "302",
        "osm_user": "way302",
        "osm_version": "21"
      },
      "refs": [
        31001,
        31002,
        31003,
        31004,
        31001
      ]
    }
  }
}

./imposm3 query-cache -rel=31101
{
  "relations": {
    "31101": {
      "tags": {
        "building": "yes",
        "osm_changeset": "3000003",
        "osm_timestamp": "2011-11-11T00:11:33Z",
        "osm_uid": "303",
        "osm_user": "rel303",
        "osm_version": "31",
        "type": "multipolygon"
      },
      "members": [
        {
          "id": 31002,
          "type": 1,
          "role": "outer"
        },
        {
          "id": 31003,
          "type": 1,
          "role": "outer"
        }
      ]
    }
  }
}


PGPASSWORD=osm psql -U osm -d imposm4 -h 172.17.42.1 -c 'select id,osm_id,osm_changeset,osm_version,osm_user,osm_uid,osm_timestamp,tags from public.osm_meta_all;'
```

 id |       osm_id        | osm_changeset | osm_version | osm_user | osm_uid |    osm_timestamp     |                                                                                    tags                                                                                     
--- | ------------------- | ------------- | ----------- | -------- | ------- | -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  1 | -100000000000031101 | 3000003       | 31          | rel303   | 303     | 2011-11-11T00:11:33Z | "osm_uid"=>"303", "building"=>"yes", "osm_user"=>"rel303", "osm_version"=>"31", "osm_changeset"=>"3000003", "osm_timestamp"=>"2011-11-11T00:11:33Z"
  2 |              -31101 | 3000002       | 21          | way302   | 302     | 2011-11-11T00:11:22Z | "highway"=>"secondary", "landuse"=>"park", "osm_uid"=>"302", "osm_user"=>"way302", "osm_version"=>"21", "osm_changeset"=>"3000002", "osm_timestamp"=>"2011-11-11T00:11:22Z"
  3 |              -31002 | 3000002       | 21          | way302   | 302     | 2011-11-11T00:11:22Z | "barrier"=>"fence", "osm_uid"=>"302", "osm_user"=>"way302", "osm_version"=>"21", "osm_changeset"=>"3000002", "osm_timestamp"=>"2011-11-11T00:11:22Z"
  4 |              -31101 | 3000002       | 21          | way302   | 302     | 2011-11-11T00:11:22Z | "highway"=>"secondary", "landuse"=>"park", "osm_uid"=>"302", "osm_user"=>"way302", "osm_version"=>"21", "osm_changeset"=>"3000002", "osm_timestamp"=>"2011-11-11T00:11:22Z"
  5 |               31101 | 3000001       | 11          | user301  | 301     | 2011-11-11T00:11:11Z | "amenity"=>"cafe", "osm_uid"=>"301", "osm_user"=>"user301", "osm_version"=>"11", "osm_changeset"=>"3000001", "osm_timestamp"=>"2011-11-11T00:11:11Z"
(5 rows)


#### imposm3 diff

```bash
./imposm3 diff -connection postgis://osm:osm@172.17.42.1/imposm4 -mapping ./test/meta_single_table_mapping.json ./test/meta_single_table.osc.gz
```

```
[Aug  3 23:15:02] [WARN] [diff] cannot find state file ./test/meta_single_table.state.txt
[Aug  3 23:15:02] [INFO] [diff] Parsing changes, updating cache and removing elements took: 2.841304ms
[Aug  3 23:15:02] [INFO] [     0] C:       0/s (5) N:       0/s (1) W:       0/s (3) R:      0/s (1)
[Aug  3 23:15:02] [INFO] [PostGIS] Updating generalized tables took: 77.23µs
[Aug  3 23:15:02] [INFO] [diff] Writing added/modified elements took: 9.129071ms
[Aug  3 23:15:02] [INFO] [diff] Processing ./test/meta_single_table.osc.gz took: 18.881671ms
[Aug  3 23:15:02] [INFO] [     0] C:       0/s (0) N:       0/s (1) W:       0/s (3) R:      0/s (1)
```

```bash
./imposm3 query-cache -node=31101
{
  "nodes": {
    "31101": {
      "tags": {
        "amenity": "restaurant",
        "osm_changeset": "4000001",
        "osm_timestamp": "2012-22-22T00:22:11Z",
        "osm_uid": "311",
        "osm_user": "user311",
        "osm_version": "12"
      },
      "lat": 47.09999997863201,
      "lon": 79.99999998675656
    }
  }
}

./imposm3 query-cache -way=31101
{
  "ways": {
    "31101": {
      "tags": {
        "highway": "secondary",
        "landuse": "park",
        "osm_changeset": "4000002",
        "osm_timestamp": "2012-22-22T00:22:22Z",
        "osm_uid": "312",
        "osm_user": "way312",
        "osm_version": "22"
      },
      "refs": [
        31001,
        31002,
        31003,
        31004,
        31001
      ]
    }
  }
}

./imposm3 query-cache -rel=31101
{
  "relations": {
    "31101": {
      "tags": {
        "amenity": "pub",
        "building": "yes",
        "osm_changeset": "4000003",
        "osm_timestamp": "2012-22-22T00:22:33Z",
        "osm_uid": "313",
        "osm_user": "rel313",
        "osm_version": "32",
        "type": "multipolygon"
      },
      "members": [
        {
          "id": 31002,
          "type": 1,
          "role": "outer"
        },
        {
          "id": 31003,
          "type": 1,
          "role": "outer"
        }
      ]
    }
  }
}
```

```bash
PGPASSWORD=osm psql -U osm -d imposm4 -h 172.17.42.1 -c 'select id,osm_id,osm_changeset,osm_version,osm_user,osm_uid,osm_timestamp,tags from public.osm_meta_all;'
```

 id |       osm_id        | osm_changeset | osm_version | osm_user | osm_uid |    osm_timestamp     |                                                                                    tags                                                                                      
--- | ------------------- | ------------- | ----------- | -------- | ------- | -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  6 |              -31002 | 4000002       | 22          | way312   | 312     | 2012-22-22T00:22:22Z | "access"=>"private", "barrier"=>"fence", "osm_uid"=>"312", "osm_user"=>"way312", "osm_version"=>"22", "osm_changeset"=>"4000002", "osm_timestamp"=>"2012-22-22T00:22:22Z"
  7 | -100000000000031101 | 4000003       | 32          | rel313   | 313     | 2012-22-22T00:22:33Z | "amenity"=>"pub", "osm_uid"=>"313", "building"=>"yes", "osm_user"=>"rel313", "osm_version"=>"32", "osm_changeset"=>"4000003", "osm_timestamp"=>"2012-22-22T00:22:33Z"
  8 |              -31101 | 4000002       | 22          | way312   | 312     | 2012-22-22T00:22:22Z | "highway"=>"secondary", "landuse"=>"park", "osm_uid"=>"312", "osm_user"=>"way312", "osm_version"=>"22", "osm_changeset"=>"4000002", "osm_timestamp"=>"2012-22-22T00:22:22Z"
  9 |               31101 | 4000001       | 12          | user311  | 311     | 2012-22-22T00:22:11Z | "amenity"=>"restaurant", "osm_uid"=>"311", "osm_user"=>"user311", "osm_version"=>"12", "osm_changeset"=>"4000001", "osm_timestamp"=>"2012-22-22T00:22:11Z"
 10 |              -31101 | 4000002       | 22          | way312   | 312     | 2012-22-22T00:22:22Z | "highway"=>"secondary", "landuse"=>"park", "osm_uid"=>"312", "osm_user"=>"way312", "osm_version"=>"22", "osm_changeset"=>"4000002", "osm_timestamp"=>"2012-22-22T00:22:22Z"
(5 rows)


