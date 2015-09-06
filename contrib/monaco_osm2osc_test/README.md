# Experimental integration test script - for testing   "import" and "diff" part. 
* status:  work in progress ;  a little ( and strange ) postgis problem 


### Method:
* import  
 * loading monaco-20150428.osm.pbf with  "imposm3_parsemetadata import" -> postgresql:public.osm_all_original 

* diff
 * convert monaco-20150428.osm.pbf -> monaco-20150428.osc with osmosis --derive-change  
 * load with "imposm3_parsemetadata diff"  -> postgresql:public.osm_all_osm2osc

The 2 database content should be equal 

compare method:
*  osm_id + tags    : tested with  linux diff command
*  osm_id + qeometry: tested with Postgis Sql 


### Problem - with geometry :
* Strange roundig error comparing: geometry ( MultiPolygon related ??? )
* The result not consistent.  Sometimes  29 rows  sometimes   30 rows    


```sql
    geom_compare_type    | osm_stgeomtype  | count 
-------------------------+-----------------+-------
 EQ_SnapToGrid_S1_0.1000 | ST_LineString   |  1191
 EQ_SnapToGrid_S1_0.1000 | ST_Point        |   466
 EQ_SnapToGrid_S1_0.1000 | ST_Polygon      |   483
 EQ_SnapToGrid_S1_0.2000 | ST_LineString   |   686
 EQ_SnapToGrid_S1_0.2000 | ST_Polygon      |   370
 EQ_SnapToGrid_S1_0.3000 | ST_LineString   |   202
 EQ_SnapToGrid_S1_0.3000 | ST_Polygon      |   112
 EQ_SnapToGrid_S1_0.4000 | ST_LineString   |   121
 EQ_SnapToGrid_S1_0.4000 | ST_Polygon      |    57
 EQ_SnapToGrid_S1_0.5000 | ST_LineString   |    35
 EQ_SnapToGrid_S1_0.5000 | ST_Polygon      |    18
 EQ_SnapToGrid_S1_0.6000 | ST_LineString   |    14
 EQ_SnapToGrid_S1_0.6000 | ST_Polygon      |     5
 EQ_SnapToGrid_S1_0.7000 | ST_LineString   |    12
 EQ_SnapToGrid_S1_0.7000 | ST_Polygon      |     9
 EQ_SnapToGrid_S1_0.8000 | ST_LineString   |     2
 EQ_SnapToGrid_S1_0.8000 | ST_Polygon      |     2
 EQ_SnapToGrid_S1_0.9000 | ST_LineString   |     2
 EQ_SnapToGrid_S1_0.9000 | ST_Polygon      |     1
 EQ_SnapToGrid_S2_0.0620 | ST_LineString   |     1
 EQ_SnapToGrid_S2_0.0620 | ST_Polygon      |     1
 EQ_SnapToGrid_S2_0.1534 | ST_LineString   |     1
 EQ_SnapToGrid_S2_0.1534 | ST_Polygon      |     1
 EQ_SnapToGrid_S2_0.1999 | ST_Polygon      |     1
 EQ_SnapToGrid_S2_0.2140 | ST_Polygon      |     1
 EQ_SnapToGrid_S2_0.3215 | ST_LineString   |     1
 EQ_SnapToGrid_S2_0.3386 | ST_Polygon      |     1
 EQ_SnapToGrid_S2_0.3745 | ST_Polygon      |     1
 EQ_SnapToGrid_S2_1.2402 | ST_MultiPolygon |     2
(29 rows)
```

but sometimes ( 30 rows )

```sql
    geom_compare_type    | osm_stgeomtype  | count 
-------------------------+-----------------+-------
 EQ_SnapToGrid_S1_0.1000 | ST_LineString   |  1191
 EQ_SnapToGrid_S1_0.1000 | ST_Point        |   466
 EQ_SnapToGrid_S1_0.1000 | ST_Polygon      |   483
 EQ_SnapToGrid_S1_0.2000 | ST_LineString   |   686
 EQ_SnapToGrid_S1_0.2000 | ST_Polygon      |   370
 EQ_SnapToGrid_S1_0.3000 | ST_LineString   |   202
 EQ_SnapToGrid_S1_0.3000 | ST_Polygon      |   112
 EQ_SnapToGrid_S1_0.4000 | ST_LineString   |   121
 EQ_SnapToGrid_S1_0.4000 | ST_Polygon      |    57
 EQ_SnapToGrid_S1_0.5000 | ST_LineString   |    35
 EQ_SnapToGrid_S1_0.5000 | ST_Polygon      |    18
 EQ_SnapToGrid_S1_0.6000 | ST_LineString   |    14
 EQ_SnapToGrid_S1_0.6000 | ST_Polygon      |     5
 EQ_SnapToGrid_S1_0.7000 | ST_LineString   |    12
 EQ_SnapToGrid_S1_0.7000 | ST_Polygon      |     9
 EQ_SnapToGrid_S1_0.8000 | ST_LineString   |     2
 EQ_SnapToGrid_S1_0.8000 | ST_Polygon      |     2
 EQ_SnapToGrid_S1_0.9000 | ST_LineString   |     2
 EQ_SnapToGrid_S1_0.9000 | ST_Polygon      |     1
 EQ_SnapToGrid_S2_0.0620 | ST_LineString   |     1
 EQ_SnapToGrid_S2_0.0620 | ST_Polygon      |     1
 EQ_SnapToGrid_S2_0.1534 | ST_LineString   |     1
 EQ_SnapToGrid_S2_0.1534 | ST_Polygon      |     1
 EQ_SnapToGrid_S2_0.1999 | ST_Polygon      |     1
 EQ_SnapToGrid_S2_0.2140 | ST_Polygon      |     1
 EQ_SnapToGrid_S2_0.3215 | ST_LineString   |     1
 EQ_SnapToGrid_S2_0.3386 | ST_Polygon      |     1
 EQ_SnapToGrid_S2_0.3745 | ST_Polygon      |     1
 EQ_SnapToGrid_S2_1.2402 | ST_MultiPolygon |     1
 EQ_SnapToGrid_S2_2.0673 | ST_MultiPolygon |     1          < ---------------  ??????
(30 rows)
```

### other info

tested on 
```sql
PostgreSQL 9.4.4 on x86_64-unknown-linux-gnu, compiled by gcc (Ubuntu 4.9.2-20ubuntu1) 4.9.2, 64-bit

POSTGIS="2.1.8 r13780" GEOS="3.4.2-CAPI-1.8.2 r3921" PROJ="Rel. 4.9.1, 04 March 2015" GDAL="GDAL 1.11.2, released 2015/02/10" LIBXML="2.9.2" LIBJSON="UNKNOWN" TOPOLOGY RASTER
```



SQL code - see :  monaco_compare_geom.sql


```sql
﻿CREATE OR REPLACE FUNCTION trytoCompareGeom(o_geometry geometry,c_geometry geometry)
    RETURNS text AS
$$
DECLARE
    strresult text;
BEGIN
   IF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 0.1,  0.1 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,0.1 ,  0.1 ))) THEN strresult := 'EQ_SnapToGrid_S1_0.1000';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 0.2,  0.2 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,0.2 ,  0.2 ))) THEN strresult := 'EQ_SnapToGrid_S1_0.2000';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 0.3,  0.3 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,0.3 ,  0.3 ))) THEN strresult := 'EQ_SnapToGrid_S1_0.3000';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 0.4,  0.4 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,0.4 ,  0.4 ))) THEN strresult := 'EQ_SnapToGrid_S1_0.4000';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 0.5,  0.5 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,0.5 ,  0.5 ))) THEN strresult := 'EQ_SnapToGrid_S1_0.5000';   
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 0.6,  0.6 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,0.6 ,  0.6 ))) THEN strresult := 'EQ_SnapToGrid_S1_0.6000';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 0.7,  0.7 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,0.7 ,  0.7 ))) THEN strresult := 'EQ_SnapToGrid_S1_0.7000';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 0.8,  0.8 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,0.8 ,  0.8 ))) THEN strresult := 'EQ_SnapToGrid_S1_0.8000';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 0.9,  0.9 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,0.9 ,  0.9 ))) THEN strresult := 'EQ_SnapToGrid_S1_0.9000';   

-- step 2 - optimized .... 
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 0.0620,  0.0620 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,0.0620 ,  0.0620 ))) THEN strresult := 'EQ_SnapToGrid_S2_0.0620';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 0.1534,  0.1534 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,0.1534 ,  0.1534 ))) THEN strresult := 'EQ_SnapToGrid_S2_0.1534';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 0.1999,  0.1999 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,0.1999 ,  0.1999 ))) THEN strresult := 'EQ_SnapToGrid_S2_0.1999';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 0.2140,  0.2140 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,0.2140 ,  0.2140 ))) THEN strresult := 'EQ_SnapToGrid_S2_0.2140';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 0.3215,  0.3215 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,0.3215 ,  0.3215 ))) THEN strresult := 'EQ_SnapToGrid_S2_0.3215';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 0.3386,  0.3386 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,0.3386 ,  0.3386 ))) THEN strresult := 'EQ_SnapToGrid_S2_0.3386';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 0.3745,  0.3745 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,0.3745 ,  0.3745 ))) THEN strresult := 'EQ_SnapToGrid_S2_0.3745';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 1.2402,  1.2402 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,1.2402 ,  1.2402 ))) THEN strresult := 'EQ_SnapToGrid_S2_1.2402';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 2.0673,  2.0673 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,2.0673 ,  2.0673 ))) THEN strresult := 'EQ_SnapToGrid_S2_2.0673';


--  step 3
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 1.0,  1.0 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,1.0 ,  1.0 ))) THEN strresult := 'EQ_SnapToGrid_S3_01.0000';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 2.0,  2.0 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,2.0 ,  2.0 ))) THEN strresult := 'EQ_SnapToGrid_S3_02.0000';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 3.0,  3.0 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,3.0 ,  3.0 ))) THEN strresult := 'EQ_SnapToGrid_S3_03.0000';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 4.0,  4.0 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,4.0 ,  4.0 ))) THEN strresult := 'EQ_SnapToGrid_S3_04.0000';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 7.0,  7.0 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,7.0 ,  7.0 ))) THEN strresult := 'EQ_SnapToGrid_S3_07.0000';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry, 9.0,  9.0 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,9.0 ,  9.0 ))) THEN strresult := 'EQ_SnapToGrid_S3_09.0000';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry,11.0, 11.0 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,11.0, 11.0 ))) THEN strresult := 'EQ_SnapToGrid_S3_11.0000';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry,13.0, 13.0 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,13.0, 13.0 ))) THEN strresult := 'EQ_SnapToGrid_S3_13.0000';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry,19.0, 19.0 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,19.0, 19.0 ))) THEN strresult := 'EQ_SnapToGrid_S3_19.0000';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry,23.0, 23.0 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,23.0, 23.0 ))) THEN strresult := 'EQ_SnapToGrid_S3_23.0000';
ELSIF  ST_AsText( ST_MakeValid( ST_SnapToGrid( o_geometry,40.0, 40.0 ))) = ST_AsText( ST_MakeValid( ST_SnapToGrid( c_geometry,40.0, 40.0 ))) THEN strresult := 'EQ_SnapToGrid_S3_40.0000';   


ELSE strresult := '___NOTEQUAL_PLEASE_CHECK!!!___';         
END IF;
RETURN strresult;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;



WITH  __osm_p as
(
select    o.osm_id
          , ST_GeometryType(o.geometry) as osm_stgeomtype   
          , trytoCompareGeom( o.geometry , c.geometry ) as geom_compare_type           
   from osm_all_original as o,
        osm_all_osm2osc  as c
   where  o.osm_id = c.osm_id
      and GeometryType(o.geometry) =  GeometryType(c.geometry)
)
select geom_compare_type,osm_stgeomtype , count(*) as count 
from __osm_p 
group by geom_compare_type,osm_stgeomtype 
order by geom_compare_type,osm_stgeomtype 
;
```


