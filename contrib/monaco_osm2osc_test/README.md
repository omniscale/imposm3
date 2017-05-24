# Experimental integration test script - for testing "import" and "diff" part - for different git versions. 

This utility script ( compare_monaco_osm2osc.sh )  only compare the OSM tags - and not yet geometry.
Tested on Linux.

Before running - check and read the scripts!  If you don't understand,  don't RUN ! 


Reguirements:
- Full dev environment , osmosis
- PostgreSQL /PostGis connection
- git     (   git checkout .. ) 
 
 
 
# Normal test -  NO git checkout - check the actual version.

From a   ./contrib/monaco_osm2osc_test directory run:   ./compare_monaco_osm2osc.sh
and check the ./output directory

output:
```
./output/201601281348_c033373__diff_tags_should_be_empty.txt 11418          <- CHECK ! 
```


# Full test with lot of git checkouts .. 

Rename the directory  to   ./contrib/monaco_osm2osc_run1
Comment out these lines from the script  
* check_old
* check_latest

Expected output  :

```
./output/201511221225_15111ca__diff_tags_should_be_empty.txt 0              <- OK, no diff
./output/201512221010_901b40b__diff_tags_should_be_empty.txt 0              <- OK, no diff
./output/201601151639_bb3d003__diff_tags_should_be_empty.txt 11418          <- CHECK ! 
./output/201601281348_c033373__diff_tags_should_be_empty.txt 11418          <- CHECK ! 
```

The zero (0) length is OK:  no difference in the "import" and "diff" part,
Otherwise - check the content of the diff file.
 
 
#IMPORTANT 
After the test please clean the git environment:
- git checkout master
- make clean 







