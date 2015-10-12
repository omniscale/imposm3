import psycopg2
import psycopg2.extras

import helper as t

psycopg2.extras.register_hstore(psycopg2.connect(**t.db_conf), globally=True)

mapping_file = 'parsemetadata_yes_mapping.json'

def setup():
    t.setup()

def teardown():
    t.teardown()

RELOFFSET = int(-1e17)

#######################################################################
def test_parsemetadata_import():
    """parsemetadata=yes test : ------------ PBF Import succeeds ------------------------------------- """
    t.drop_schemas()
    assert not t.table_exists('osm_parsemetadata', schema=t.TEST_SCHEMA_IMPORT)
    t.imposm3_import(t.db_conf, './build/parsemetadata_data_withmeta.pbf', mapping_file)
    assert t.table_exists('osm_parsemetadata', schema=t.TEST_SCHEMA_IMPORT)

def test_parsemetadata_deploy():
    """parsemetadata=yes test : ------------ Deploy succeeds ----------------------------------------- """
    assert not t.table_exists('osm_parsemetadata', schema=t.TEST_SCHEMA_PRODUCTION)
    t.imposm3_deploy(t.db_conf, mapping_file)
    assert t.table_exists('osm_parsemetadata', schema=t.TEST_SCHEMA_PRODUCTION)
    assert not t.table_exists('osm_parsemetadata', schema=t.TEST_SCHEMA_IMPORT)


def test_parsemetadata_pbf_created_by():
    """parsemetadata=yes test : PBF-node key:created_by test   ( config.ParseDontAddOnlyCreatedByTag )  """     

    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31002 ) == None
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31003 ) == None
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31004 ) == None

    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31001)
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31001)['tags']['created_by'] == "JOSM" 
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31101)
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31101)['tags']['created_by'] == "iDEditor"
 

def test_parsemetadata_pbf_n31101():
    """parsemetadata=yes test : PBF-node osm_id=n31101 : keys:testnote_*  should equal with keys:osm_*  """     
    element = t.query_row(t.db_conf, 'osm_parsemetadata', 31101)
    assert element['osm_changeset']         == element['tags']['testnote_changeset']  
    assert element['osm_version']           == element['tags']['testnote_version']  
    assert element['osm_user']              == element['tags']['testnote_user']  
    assert element['osm_uid']               == element['tags']['testnote_uid']  
    assert element['osm_timestamp']         == element['tags']['testnote_timestamp']  
    
    assert element['tags']['osm_changeset'] == element['tags']['testnote_changeset']  
    assert element['tags']['osm_version']   == element['tags']['testnote_version']  
    assert element['tags']['osm_user']      == element['tags']['testnote_user']  
    assert element['tags']['osm_uid']       == element['tags']['testnote_uid']  
    assert element['tags']['osm_timestamp'] == element['tags']['testnote_timestamp']  

def test_parsemetadata_pbf_w31101():
    """parsemetadata=yes test : PBF-way  osm_id=w31101 : keys:testnote_*  should equal with keys:osm_*  """      
    element = t.query_row(t.db_conf, 'osm_parsemetadata', -31101)
    assert element['osm_changeset']         == element['tags']['testnote_changeset']  
    assert element['osm_version']           == element['tags']['testnote_version']  
    assert element['osm_user']              == element['tags']['testnote_user']  
    assert element['osm_uid']               == element['tags']['testnote_uid']  
    assert element['osm_timestamp']         == element['tags']['testnote_timestamp']  
    
    assert element['tags']['osm_changeset'] == element['tags']['testnote_changeset']  
    assert element['tags']['osm_version']   == element['tags']['testnote_version']  
    assert element['tags']['osm_user']      == element['tags']['testnote_user']  
    assert element['tags']['osm_uid']       == element['tags']['testnote_uid']  
    assert element['tags']['osm_timestamp'] == element['tags']['testnote_timestamp']  

def test_parsemetadata_pbf_w31002():
    """parsemetadata=yes test : PBF-way  osm_id=w31002 : keys:testnote_*  should equal with keys:osm_*  """  
    element = t.query_row(t.db_conf, 'osm_parsemetadata',  -31002)
    assert element['osm_changeset']         == element['tags']['testnote_changeset']  
    assert element['osm_version']           == element['tags']['testnote_version']  
    assert element['osm_user']              == element['tags']['testnote_user']  
    assert element['osm_uid']               == element['tags']['testnote_uid']  
    assert element['osm_timestamp']         == element['tags']['testnote_timestamp'] 
     
    assert element['tags']['osm_changeset'] == element['tags']['testnote_changeset']  
    assert element['tags']['osm_version']   == element['tags']['testnote_version']  
    assert element['tags']['osm_user']      == element['tags']['testnote_user']  
    assert element['tags']['osm_uid']       == element['tags']['testnote_uid']  
    assert element['tags']['osm_timestamp'] == element['tags']['testnote_timestamp']  

def test_parsemetadata_pbf_r31101():
    """parsemetadata=yes test : PBF-rel  osm_id=r31101 : keys:testnote_*  should equal with keys:osm_*  """ 
    element = t.query_row(t.db_conf, 'osm_parsemetadata',  -100000000000031101)
    assert element['osm_changeset']         == element['tags']['testnote_changeset']  
    assert element['osm_version']           == element['tags']['testnote_version']  
    assert element['osm_user']              == element['tags']['testnote_user']  
    assert element['osm_uid']               == element['tags']['testnote_uid']  
    assert element['osm_timestamp']         == element['tags']['testnote_timestamp']  
    
    assert element['tags']['osm_changeset'] == element['tags']['testnote_changeset']  
    assert element['tags']['osm_version']   == element['tags']['testnote_version']  
    assert element['tags']['osm_user']      == element['tags']['testnote_user']  
    assert element['tags']['osm_uid']       == element['tags']['testnote_uid']  
    assert element['tags']['osm_timestamp'] == element['tags']['testnote_timestamp']  
 
   
#######################################################################

def test_parsemetadata_update():
    """parsemetadata=yes test : ------------ OSC import applies -------------------------------------- """
    t.imposm3_update(t.db_conf, './build/parsemetadata_data.osc.gz', mapping_file)

#######################################################################

def test_parsemetadata_osc_created_by():
    """parsemetadata=yes test : PBF-node key:created_by test   ( config.ParseDontAddOnlyCreatedByTag )  """  
    
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31001 ) == None
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31002)
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31002)['tags']['created_by'] == "iDEditor"
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31003 ) == None
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31004 ) == None
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31101)
    assert t.query_row(t.db_conf, 'osm_parsemetadata', 31101)['tags']['created_by'] == "JOSM" 


def test_parsemetadata_osc_n31101():
    """parsemetadata=yes test : OSC-node osm_id=n31101 : keys:testnote_*  should equal with keys:osm_*  """
    element = t.query_row(t.db_conf, 'osm_parsemetadata', 31101)
    assert element['osm_changeset']         == element['tags']['testnote_changeset']  
    assert element['osm_version']           == element['tags']['testnote_version']  
    assert element['osm_user']              == element['tags']['testnote_user']  
    assert element['osm_uid']               == element['tags']['testnote_uid']  
    assert element['osm_timestamp']         == element['tags']['testnote_timestamp']  
    
    assert element['tags']['osm_changeset'] == element['tags']['testnote_changeset']  
    assert element['tags']['osm_version']   == element['tags']['testnote_version']  
    assert element['tags']['osm_user']      == element['tags']['testnote_user']  
    assert element['tags']['osm_uid']       == element['tags']['testnote_uid']  
    assert element['tags']['osm_timestamp'] == element['tags']['testnote_timestamp']  


def test_parsemetadata_osc_w31101():
    """parsemetadata=yes test : OSC-way  osm_id=w31101 : keys:testnote_*  should equal with keys:osm_* """    
    aelement = t.query_row(t.db_conf, 'osm_parsemetadata', -31101)
    element=aelement[0]
    assert element['osm_changeset']         == element['tags']['testnote_changeset']  
    assert element['osm_version']           == element['tags']['testnote_version']  
    assert element['osm_user']              == element['tags']['testnote_user']  
    assert element['osm_uid']               == element['tags']['testnote_uid']  
    assert element['osm_timestamp']         == element['tags']['testnote_timestamp']  
    
    assert element['tags']['osm_changeset'] == element['tags']['testnote_changeset']  
    assert element['tags']['osm_version']   == element['tags']['testnote_version']  
    assert element['tags']['osm_user']      == element['tags']['testnote_user']  
    assert element['tags']['osm_uid']       == element['tags']['testnote_uid']  
    assert element['tags']['osm_timestamp'] == element['tags']['testnote_timestamp']  

    element=aelement[1]
    assert element['osm_changeset']         == element['tags']['testnote_changeset']  
    assert element['osm_version']           == element['tags']['testnote_version']  
    assert element['osm_user']              == element['tags']['testnote_user']  
    assert element['osm_uid']               == element['tags']['testnote_uid']  
    assert element['osm_timestamp']         == element['tags']['testnote_timestamp']  
    
    assert element['tags']['osm_changeset'] == element['tags']['testnote_changeset']  
    assert element['tags']['osm_version']   == element['tags']['testnote_version']  
    assert element['tags']['osm_user']      == element['tags']['testnote_user']  
    assert element['tags']['osm_uid']       == element['tags']['testnote_uid']  
    assert element['tags']['osm_timestamp'] == element['tags']['testnote_timestamp']  
 
def test_parsemetadata_osc_w31002():
    """parsemetadata=yes test : OSC-way  osm_id=w31002 : keys:testnote_*  should equal with keys:osm_* """  
    element = t.query_row(t.db_conf, 'osm_parsemetadata',  -31002)
    assert element['osm_changeset']         == element['tags']['testnote_changeset']  
    assert element['osm_version']           == element['tags']['testnote_version']  
    assert element['osm_user']              == element['tags']['testnote_user']  
    assert element['osm_uid']               == element['tags']['testnote_uid']  
    assert element['osm_timestamp']         == element['tags']['testnote_timestamp'] 
     
    assert element['tags']['osm_changeset'] == element['tags']['testnote_changeset']  
    assert element['tags']['osm_version']   == element['tags']['testnote_version']  
    assert element['tags']['osm_user']      == element['tags']['testnote_user']  
    assert element['tags']['osm_uid']       == element['tags']['testnote_uid']  
    assert element['tags']['osm_timestamp'] == element['tags']['testnote_timestamp']  

def test_parsemetadata_osc_r31101():
    """parsemetadata=yes test : OSC-rel  osm_id=r31101 : keys:testnote_*  should equal with keys:osm_* """  
    element = t.query_row(t.db_conf, 'osm_parsemetadata',  -100000000000031101)
    assert element['osm_changeset']         == element['tags']['testnote_changeset']  
    assert element['osm_version']           == element['tags']['testnote_version']  
    assert element['osm_user']              == element['tags']['testnote_user']  
    assert element['osm_uid']               == element['tags']['testnote_uid']  
    assert element['osm_timestamp']         == element['tags']['testnote_timestamp']  
    
    assert element['tags']['osm_changeset'] == element['tags']['testnote_changeset']  
    assert element['tags']['osm_version']   == element['tags']['testnote_version']  
    assert element['tags']['osm_user']      == element['tags']['testnote_user']  
    assert element['tags']['osm_uid']       == element['tags']['testnote_uid']  
    assert element['tags']['osm_timestamp'] == element['tags']['testnote_timestamp']  
 
   




