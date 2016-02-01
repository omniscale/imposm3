# Experimental integration test script - for testing "import" and "diff" part - for different git versions. 

This (compare_monaco_osm2osc.sh ) script only compare the OSM tags - and not yet geometry.
Tested on Linux.

Before running - check the scripts! 

Reguirements:
- Full dev enviromnent 
- PostgreSQL /PostGis connection
- git     (   git checkout .. ) 
 
 
From a   ./contrib/monaco_osm2osc_test directory run:   ./compare_monaco_osm2osc.sh
and check the ./output directory


Expected output and example :

```
./output/201511221225_15111ca__diff_tags_should_be_empty.txt 0              <- OK, no diff
./output/201512221010_901b40b__diff_tags_should_be_empty.txt 0              <- OK, no diff
./output/201601151639_bb3d003__diff_tags_should_be_empty.txt 11418          <- CHECK ! 
./output/201601281348_c033373__diff_tags_should_be_empty.txt 11418          <- CHECK ! 
```

The zero (0) length is OK:  no difference in the "import" and "diff" part,
Otherwise - check the content of the diff file.
 
 
IMPORTANT : after the test please clean the environment:
- git checkout master
- make clean 







