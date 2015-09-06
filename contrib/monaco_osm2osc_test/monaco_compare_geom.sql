CREATE OR REPLACE FUNCTION trytoCompareGeom(o_geometry geometry,c_geometry geometry)
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
