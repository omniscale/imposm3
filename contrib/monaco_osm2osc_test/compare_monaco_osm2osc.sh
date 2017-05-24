#!/bin/bash
set -x

#  Simple Monaco test -  for testing  "import" and "diff" part same behaviour.
#
#  this script overwrite 2 postgres table,  be careful!  
#  -  public.osm_all_original
#  -  public.osm_all_osm2osc

TESTVERSION=0
BUILD_ID=0
BUILD_LASTMOD=0
BUILD_REV=0

impconnection=postgis://$PGHOST/$PGDATABASE 
monaco_osm_latest_pbf=./build/monaco-160101.osm.pbf
monaco_osm_zero_pbf=../../parser/pbf/monaco-20150428.osm.pbf
monaco_osm_zero_osc=./build/monaco_diff_20150428_20150101.osc

workdir=$(pwd)


mkdir -p ./output
mkdir -p ./build
rm -f ./output/*.txt
# rm -f ./build/*

# Download newer Monaco data ... 
if [ ! -f $monaco_osm_latest_pbf  ]; then
    echo "File not found!"
    wget -O $monaco_osm_latest_pbf   http://download.geofabrik.de/europe/monaco-160101.osm.pbf
fi


rm -f ${monaco_osm_zero_osc}.gz

# Check monaco OSC file ... 
if [ ! -f ${monaco_osm_zero_osc}.gz   ]; then
   # --- create OSC file  with osmosis derive-change
   osmosis -q --read-pbf file="$monaco_osm_latest_pbf" \
              --read-pbf file="$monaco_osm_zero_pbf" \
              --derive-change \
              --write-xml-change file="$monaco_osm_zero_osc"           
   cat  $monaco_osm_zero_osc  | gzip > ${monaco_osm_zero_osc}.gz
fi



# Big TEST function ....
function runtest {

  cd $workdir
  
  BUILD_REV=$(git rev-parse --short HEAD)
  BUILD_LASTMOD=$(date -d @$(git log -n1 --format="%at") +%Y%m%d%H%M )
  BUILD_ID=${BUILD_LASTMOD}_${BUILD_REV}_
  
  echo   BUILD_REV=$BUILD_REV
  echo   BUILD_LASTMOD=$BUILD_LASTMOD
  echo   BUILD_ID=$BUILD_ID
  
  
  # check last git log :
  git log -1 > ./output/${BUILD_ID}_git_latest_log.txt 

  # check imposm3 version :
  ../../imposm3 version   > ./output/${BUILD_ID}_imposm3_version.txt

  # -- create   public.osm_all_original  with 1 step
  ../../imposm3 import  -mapping monaco_mapping_original.json  \
                                    -read $monaco_osm_latest_pbf \
                                    -diff     \
                                    -write    \
                                    -optimize \
                                    -overwritecache \
                                    -deployproduction \
                                    -connection $impconnection



  # -- create  public.osm_all_osm2osc  with 2 step
  echo ' ----  import ----'
  ../../imposm3 import  -mapping monaco_mapping_osm2osc.json \
                                    -read $monaco_osm_zero_pbf  \
                                    -diff \
                                    -write \
                                    -optimize \
                                    -overwritecache \
                                    -deployproduction \
                                    -connection $impconnection 
                                    
  echo ' ----  diff ----'                                    
  ../../imposm3 diff    -mapping monaco_mapping_osm2osc.json \
                                    -connection $impconnection  \
                                    ${monaco_osm_zero_osc}.gz


  # check the mapping files ...  only 1 line can be different - the  table name !!
  diff monaco_mapping_original.json monaco_mapping_osm2osc.json  > ./output/${BUILD_ID}_diff_mapping_json.txt


  # ---- compare    osm_id + tags
  psql -c "COPY ( select osm_id, tags  from  public.osm_all_original order by osm_id, tags ) TO STDOUT" \
       > ./output/${BUILD_ID}_osm_all_original_tags.txt

  psql -c "COPY ( select osm_id, tags  from  public.osm_all_osm2osc  order by osm_id, tags ) TO STDOUT" \
       > ./output/${BUILD_ID}_osm_all_osm2osc_tags.txt
      
      
  # this file should be empty - if the  "import" and "diff"  part is the same! 
  diff ./output/${BUILD_ID}_osm_all_original_tags.txt ./output/${BUILD_ID}_osm_all_osm2osc_tags.txt  > ./output/${BUILD_ID}_diff_tags_should_be_empty.txt

  echo " this should be empty !!!" 
  cat  ./output/${BUILD_ID}_diff_tags_should_be_empty.txt  | tail -10

}



# build special TEST version
function testgitversion {
  cd ../../
  git checkout $TESTVERSION 
  make clean
  godep go clean -i -r
  make build
  cd $workdir
}


function check_old {
 VERSIONDATE=#2016jan18  TESTVERSION=bb3d003bf6e0e3593dd5b3f9aacb3518e76d6142  testgitversion && runtest
 VERSIONDATE=#2015Dec22  TESTVERSION=901b40bfd7e9d0eace5c74903d64731fdae2fafa  testgitversion && runtest
 VERSIONDATE=#2015nov22  TESTVERSION=15111cab7a21c5debe14f2841d7c9b66031dd8b7  testgitversion && runtest
}

function check_latest {
 VERSIONDATE=#latest     TESTVERSION=master                                    testgitversion && runtest
}


function check  {
# no git checkout ...
 VERSIONDATE=#current    TESTVERSION=current  runtest
}


##  Run from a renamed directory like  /contrib/monaco_osm2osc_run1  - because "git checkout" remove some files ..
#--------------------------
#check_old
#check_latest


# for testing actual version - no git checkout 
check

echo "Filenames start with  LastGitDatetime_GitHash prefix, like:  /201508071417_29a4d4e__..."
echo "Test results:  zero length OK ,  not zero BAD!" 
ls -Xl ./output/*_diff_tags_should_be_empty.txt | awk '{print $9, $5}'




















