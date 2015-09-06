#!/bin/bash
set -ex


#
#  experimental test script!
#
#
#  this script overwrite 2 postgres table,  be careful!  
#  -  public.osm_all_original
#  -  public.osm_all_osm2osc
# 

mkdir -p ./output

impconnection=postgis://$PGHOST/$PGDATABASE 

monaco_osm_pbf=../../parser/pbf/monaco-20150428.osm.pbf
monaco_osm_zero_pbf=../../test/build/parsemetadata_data_withmeta.pbf
monaco_osm_zero_osc=../../test/build/monaco_diff_20150428.osc



# --------------  TEST  OSM  branch  -----------------------------
#  read  "monaco-20150428.osm.pbf"   -->  public.osm_all_original
../../imposm3_parsemetadata import  -mapping monaco_mapping_original.json  \
                                    -read $monaco_osm_pbf \
                                    -diff     \
                                    -write    \
                                    -optimize \
                                    -overwritecache \
                                    -deployproduction \
                                    -connection $impconnection

#
#                                  Table "public.osm_all_original"
#  Column  |          Type           |                           Modifiers                           
#----------+-------------------------+---------------------------------------------------------------
# id       | integer                 | not null default nextval('osm_all_original_id_seq'::regclass)
# osm_id   | bigint                  | 
# tags     | hstore                  | 
# geometry | geometry(Geometry,3857) | 
#Indexes:
#    "osm_all_original_pkey" PRIMARY KEY, btree (id)
#    "osm_all_original_geom" gist (geometry)
#    "osm_all_original_geom_geohash" btree (st_geohash(st_transform(st_setsrid(box2d(geometry)::geometry, 3857), 4326))) CLUSTER
#    "osm_all_original_osm_id_idx" btree (osm_id)
#
#
#






# --------------  TEST  OSM -> OSC  branch  -----------------------------
#  create  osc file from  "monaco-20150428.osm.pbf"  ( with osmosis derive-change )
#


# --- prepare data
osmosis -q --read-xml ../../test/parsemetadata_data.osm --write-pbf $monaco_osm_zero_pbf

osmosis -q --read-pbf file="$monaco_osm_pbf" \
           --read-pbf file="$monaco_osm_zero_pbf" \
           --derive-change \
           --write-xml-change file="$monaco_osm_zero_osc"
           
cat  $monaco_osm_zero_osc  | gzip > ${monaco_osm_zero_osc}.gz



# --- load data    public.osm_all_osm2osc
echo ' ----  import ----'
../../imposm3_parsemetadata import  -mapping monaco_mapping_osm2osc.json \
                                    -read $monaco_osm_zero_pbf  \
                                    -diff \
                                    -write \
                                    -optimize \
                                    -overwritecache \
                                    -deployproduction \
                                    -connection $impconnection 
                                    
echo ' ----  diff ----'                                    
../../imposm3_parsemetadata diff    -mapping monaco_mapping_osm2osc.json \
                                    -connection $impconnection  \
                                    ${monaco_osm_zero_osc}.gz


echo ' ---- '   
#                                  Table "public.osm_all_osm2osc"
#  Column  |          Type           |                          Modifiers                           
#----------+-------------------------+--------------------------------------------------------------
# id       | integer                 | not null default nextval('osm_all_osm2osc_id_seq'::regclass)
# osm_id   | bigint                  | 
# tags     | hstore                  | 
# geometry | geometry(Geometry,3857) | 
#Indexes:
#    "osm_all_osm2osc_pkey" PRIMARY KEY, btree (id)
#    "osm_all_osm2osc_geom" gist (geometry)
#    "osm_all_osm2osc_geom_geohash" btree (st_geohash(st_transform(st_setsrid(box2d(geometry)::geometry, 3857), 4326))) CLUSTER
#    "osm_all_osm2osc_osm_id_idx" btree (osm_id)
#
#





# ---- compare    osm_id + tags
psql -c "COPY ( select osm_id, tags  from  public.osm_all_original order by osm_id, tags ) TO STDOUT" \
      > ./output/_osm_all_original_tags.txt

psql -c "COPY ( select osm_id, tags  from  public.osm_all_osm2osc  order by osm_id, tags ) TO STDOUT" \
      > ./output/_osm_all_osm2osc_tags.txt
      
      
diff ./output/_osm_all_original_tags.txt ./output/_osm_all_osm2osc_tags.txt  > ./output/_diff_tags_should_be_empty.txt

# this file should be empty! 
cat  ./output/_diff_tags_should_be_empty.txt  | tail -20 


# ---- compare     osm_id + geometry 
# method:       ST_AsText( ST_MakeValid( ST_SnapToGrid( geometry, .., ... )))    eq ... 
#
 
 
psql -f "monaco_compare_geom.sql" > ./output/_monaco_compare_geom_output.txt
cat ./output/_monaco_compare_geom_output.txt


# expected output:
#
#     geom_compare_type   | osm_stgeomtype  | count
#   ----------------------+-----------------+-------
#    EQ_SnapToGrid_0.0620 | ST_LineString   |     1
#    EQ_SnapToGrid_0.0620 | ST_Polygon      |     1
#    EQ_SnapToGrid_0.1000 | ST_LineString   |  1191
#    EQ_SnapToGrid_0.1000 | ST_Point        |   466
#    EQ_SnapToGrid_0.1000 | ST_Polygon      |   483
#    EQ_SnapToGrid_0.1534 | ST_LineString   |     1
#    EQ_SnapToGrid_0.1534 | ST_Polygon      |     1
#    EQ_SnapToGrid_0.1999 | ST_Polygon      |     1
#    EQ_SnapToGrid_0.2000 | ST_LineString   |   686
#    EQ_SnapToGrid_0.2000 | ST_Polygon      |   370
#    EQ_SnapToGrid_0.2140 | ST_Polygon      |     1
#    EQ_SnapToGrid_0.3000 | ST_LineString   |   202
#    EQ_SnapToGrid_0.3000 | ST_Polygon      |   112
#    EQ_SnapToGrid_0.3215 | ST_LineString   |     1
#    EQ_SnapToGrid_0.3386 | ST_Polygon      |     1
#    EQ_SnapToGrid_0.3745 | ST_Polygon      |     1
#    EQ_SnapToGrid_0.4000 | ST_LineString   |   121
#    EQ_SnapToGrid_0.4000 | ST_Polygon      |    57
#    EQ_SnapToGrid_0.5000 | ST_LineString   |    35
#    EQ_SnapToGrid_0.5000 | ST_Polygon      |    18
#    EQ_SnapToGrid_0.6000 | ST_LineString   |    14
#    EQ_SnapToGrid_0.6000 | ST_Polygon      |     5
#    EQ_SnapToGrid_0.7000 | ST_LineString   |    12
#    EQ_SnapToGrid_0.7000 | ST_Polygon      |     9
#    EQ_SnapToGrid_0.8000 | ST_LineString   |     2
#    EQ_SnapToGrid_0.8000 | ST_Polygon      |     2
#    EQ_SnapToGrid_0.9000 | ST_LineString   |     2
#    EQ_SnapToGrid_0.9000 | ST_Polygon      |     1
#   (29 rows)
#
#

psql -c "COPY ( SELECT version()              ) TO STDOUT"   > ./output/_postgresql_version.txt
psql -c "COPY ( SELECT postgis_full_version() ) TO STDOUT"   > ./output/_postgis_version.txt

    




































