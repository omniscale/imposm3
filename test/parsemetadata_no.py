import psycopg2
import psycopg2.extras

import helper as t

psycopg2.extras.register_hstore(psycopg2.connect(**t.db_conf), globally=True)

mapping_file = 'parsemetadata_no_mapping.json'

def setup():
    t.setup()

def teardown():
    t.teardown()

RELOFFSET = int(-1e17)

#######################################################################
def test_parsemetadata_import():
    """parsemetadata=no  test : ------------ PBF Import succeeds ------------------------------------- """
    t.drop_schemas()
    assert not t.table_exists('osm_parsemetadata', schema=t.TEST_SCHEMA_IMPORT)
    t.imposm3_import(t.db_conf, './build/parsemetadata_data_withmeta.pbf', mapping_file)
    assert t.table_exists('osm_parsemetadata', schema=t.TEST_SCHEMA_IMPORT)

def test_parsemetadata_deploy():
    """parsemetadata=no  test : ------------ Deploy succeeds ----------------------------------------- """
    assert not t.table_exists('osm_parsemetadata', schema=t.TEST_SCHEMA_PRODUCTION)
    t.imposm3_deploy(t.db_conf, mapping_file)
    assert t.table_exists('osm_parsemetadata', schema=t.TEST_SCHEMA_PRODUCTION)
    assert not t.table_exists('osm_parsemetadata', schema=t.TEST_SCHEMA_IMPORT)


def test_parsemetadata_pbf_created_by():
    """parsemetadata=no  test : PBF-node key:created_by test   ( config.ParseDontAddOnlyCreatedByTag )  """     

    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31001 ) == None
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31002 ) == None
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31003 ) == None
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31004 ) == None

    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31101)
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31101)['tags']['created_by'] == "iDEditor"
 


def test_parsemetadata_pbf_n1():
    """parsemetadata=no  test : PBF-node osm_id=n1 :  meta keys will not have been overwritten ------- """      
    element = t.query_row(t.db_conf, 'osm_parsemetadata', 1)
    
    assert element['_changeset_']             == "OpenStreetMap_node_osm_changeset"   
    assert element['_version_']               == "OpenStreetMap_node_osm_version"
    assert element['_user_']                  == "OpenStreetMap_node_osm_user" 
    assert element['_uid_']                   == "OpenStreetMap_node_osm_uid"  
    assert element['_timestamp_']             == "OpenStreetMap_node_osm_timestamp"




def test_parsemetadata_pbf_n1():
    """parsemetadata=no  test : PBF-node osm_id=n1 :  meta keys will not have been overwritten ------- """      
    element = t.query_row(t.db_conf, 'osm_parsemetadata', 1)
    
    assert element['_changeset_']             == "OpenStreetMap_node_osm_changeset"   
    assert element['_version_']               == "OpenStreetMap_node_osm_version"
    assert element['_user_']                  == "OpenStreetMap_node_osm_user" 
    assert element['_uid_']                   == "OpenStreetMap_node_osm_uid"  
    assert element['_timestamp_']             == "OpenStreetMap_node_osm_timestamp"
  
def test_parsemetadata_pbf_w1():
    """parsemetadata=no  test : PBF-way  osm_id=w1 :  meta keys will not have been overwritten ------- """     
    element = t.query_row(t.db_conf, 'osm_parsemetadata', -1)
    
    assert element['_changeset_']             == "OpenStreetMap_way_osm_changeset"   
    assert element['_version_']               == "OpenStreetMap_way_osm_version"
    assert element['_user_']                  == "OpenStreetMap_way_osm_user" 
    assert element['_uid_']                   == "OpenStreetMap_way_osm_uid"  
    assert element['_timestamp_']             == "OpenStreetMap_way_osm_timestamp"
  
def test_parsemetadata_pbf_r1():
    """parsemetadata=no  test : PBF-rel  osm_id=r1 :  meta keys will not have been overwritten ------- """     
    element = t.query_row(t.db_conf, 'osm_parsemetadata', -100000000000000001  )
    
    assert element['_changeset_']             == "OpenStreetMap_rel_osm_changeset"   
    assert element['_version_']               == "OpenStreetMap_rel_osm_version"
    assert element['_user_']                  == "OpenStreetMap_rel_osm_user" 
    assert element['_uid_']                   == "OpenStreetMap_rel_osm_uid"  
    assert element['_timestamp_']             == "OpenStreetMap_rel_osm_timestamp"
  

def test_parsemetadata_pbf_n31101():
    """parsemetadata=no  test : PBF-node osm_id=n31101 :  meta keys  should be empty -----------------  """     
    element = t.query_row(t.db_conf, 'osm_parsemetadata', 31101)
    
    assert element['_changeset_']             == ""   
    assert element['_version_']               == ""  
    assert element['_user_']                  == ""  
    assert element['_uid_']                   == ""  
    assert element['_timestamp_']             == ""  
    
    assert not element['_changeset_']         == element['tags']['testnote_changeset']  
    assert not element['_version_']           == element['tags']['testnote_version']  
    assert not element['_user_']              == element['tags']['testnote_user']  
    assert not element['_uid_']               == element['tags']['testnote_uid']  
    assert not element['_timestamp_']         == element['tags']['testnote_timestamp']  
    


def test_parsemetadata_pbf_w31101():
    """parsemetadata=no  test : PBF-way  osm_id=w31101 :  meta keys  should be empty -----------------  """      
    element = t.query_row(t.db_conf, 'osm_parsemetadata', -31101)
    assert element['_changeset_']             == ""   
    assert element['_version_']               == ""  
    assert element['_user_']                  == ""  
    assert element['_uid_']                   == ""  
    assert element['_timestamp_']             == ""  
    
    assert not element['_changeset_']         == element['tags']['testnote_changeset']  
    assert not element['_version_']           == element['tags']['testnote_version']  
    assert not element['_user_']              == element['tags']['testnote_user']  
    assert not element['_uid_']               == element['tags']['testnote_uid']  
    assert not element['_timestamp_']         == element['tags']['testnote_timestamp']  
   
def test_parsemetadata_pbf_w31002():
    """parsemetadata=no  test : PBF-way  osm_id=w31002 :  meta keys  should be empty -----------------  """  
    element = t.query_row(t.db_conf, 'osm_parsemetadata',  -31002)
    assert element['_changeset_']             == ""   
    assert element['_version_']               == ""  
    assert element['_user_']                  == ""  
    assert element['_uid_']                   == ""  
    assert element['_timestamp_']             == ""  
    
    assert not element['_changeset_']         == element['tags']['testnote_changeset']  
    assert not element['_version_']           == element['tags']['testnote_version']  
    assert not element['_user_']              == element['tags']['testnote_user']  
    assert not element['_uid_']               == element['tags']['testnote_uid']  
    assert not element['_timestamp_']         == element['tags']['testnote_timestamp']  
   

def test_parsemetadata_pbf_r31101():
    """parsemetadata=no  test : PBF-rel  osm_id=r31101 :  meta keys  should be empty -----------------  """ 
    element = t.query_row(t.db_conf, 'osm_parsemetadata',  -100000000000031101)
    assert element['_changeset_']             == ""   
    assert element['_version_']               == ""  
    assert element['_user_']                  == ""  
    assert element['_uid_']                   == ""  
    assert element['_timestamp_']             == ""  
    
    assert not element['_changeset_']         == element['tags']['testnote_changeset']  
    assert not element['_version_']           == element['tags']['testnote_version']  
    assert not element['_user_']              == element['tags']['testnote_user']  
    assert not element['_uid_']               == element['tags']['testnote_uid']  
    assert not element['_timestamp_']         == element['tags']['testnote_timestamp']  
   
 
   
#######################################################################

def test_parsemetadata_update():
    """parsemetadata=no  test : ------------ OSC import applies -------------------------------------- """
    t.imposm3_update(t.db_conf, './build/parsemetadata_data.osc.gz', mapping_file)

#######################################################################


def test_parsemetadata_osc_created_by():
    """parsemetadata=no  test : PBF-node key:created_by test   ( config.ParseDontAddOnlyCreatedByTag )  """  
    
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31001 ) == None
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31002 ) == None
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31003 ) == None
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31004 ) == None
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31101)
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31101)['tags']['created_by'] == "JOSM" 



def test_parsemetadata_osc_n31101():
    """parsemetadata=no  test : OSC-node osm_id=n31101 :  meta keys  should be empty -----------------  """
    element = t.query_row(t.db_conf, 'osm_parsemetadata', 31101)
    assert element['_changeset_']             == ""   
    assert element['_version_']               == ""  
    assert element['_user_']                  == ""  
    assert element['_uid_']                   == ""  
    assert element['_timestamp_']             == ""  
    
    assert not element['_changeset_']         == element['tags']['testnote_changeset']  
    assert not element['_version_']           == element['tags']['testnote_version']  
    assert not element['_user_']              == element['tags']['testnote_user']  
    assert not element['_uid_']               == element['tags']['testnote_uid']  
    assert not element['_timestamp_']         == element['tags']['testnote_timestamp']  
   


def test_parsemetadata_osc_w31101():
    """parsemetadata=no  test : OSC-way  osm_id=w31101 :  meta keys  should be empty ----------------- """    
    aelement = t.query_row(t.db_conf, 'osm_parsemetadata', -31101)
    element=aelement[0]
    assert element['_changeset_']             == ""   
    assert element['_version_']               == ""  
    assert element['_user_']                  == ""  
    assert element['_uid_']                   == ""  
    assert element['_timestamp_']             == ""  
    
    assert not element['_changeset_']         == element['tags']['testnote_changeset']  
    assert not element['_version_']           == element['tags']['testnote_version']  
    assert not element['_user_']              == element['tags']['testnote_user']  
    assert not element['_uid_']               == element['tags']['testnote_uid']  
    assert not element['_timestamp_']         == element['tags']['testnote_timestamp']  
   

    element=aelement[1]
    assert element['_changeset_']             == ""   
    assert element['_version_']               == ""  
    assert element['_user_']                  == ""  
    assert element['_uid_']                   == ""  
    assert element['_timestamp_']             == ""  
    
    assert not element['_changeset_']         == element['tags']['testnote_changeset']  
    assert not element['_version_']           == element['tags']['testnote_version']  
    assert not element['_user_']              == element['tags']['testnote_user']  
    assert not element['_uid_']               == element['tags']['testnote_uid']  
    assert not element['_timestamp_']         == element['tags']['testnote_timestamp']  
   
 
def test_parsemetadata_osc_w31002():
    """parsemetadata=no  test : OSC-way  osm_id=w31002 :  meta keys  should be empty ----------------- """  
    element = t.query_row(t.db_conf, 'osm_parsemetadata',  -31002)
    assert element['_changeset_']             == ""   
    assert element['_version_']               == ""  
    assert element['_user_']                  == ""  
    assert element['_uid_']                   == ""  
    assert element['_timestamp_']             == ""  
    
    assert not element['_changeset_']         == element['tags']['testnote_changeset']  
    assert not element['_version_']           == element['tags']['testnote_version']  
    assert not element['_user_']              == element['tags']['testnote_user']  
    assert not element['_uid_']               == element['tags']['testnote_uid']  
    assert not element['_timestamp_']         == element['tags']['testnote_timestamp']  
    

def test_parsemetadata_osc_r31101():
    """parsemetadata=no  test : OSC-rel  osm_id=r31101 :  meta keys  should be empty ----------------- """  
    element = t.query_row(t.db_conf, 'osm_parsemetadata',  -100000000000031101)
    assert element['_changeset_']             == ""   
    assert element['_version_']               == ""  
    assert element['_user_']                  == ""  
    assert element['_uid_']                   == ""  
    assert element['_timestamp_']             == ""  
    
    assert not element['_changeset_']         == element['tags']['testnote_changeset']  
    assert not element['_version_']           == element['tags']['testnote_version']  
    assert not element['_user_']              == element['tags']['testnote_user']  
    assert not element['_uid_']               == element['tags']['testnote_uid']  
    assert not element['_timestamp_']         == element['tags']['testnote_timestamp']  
   
 
   




