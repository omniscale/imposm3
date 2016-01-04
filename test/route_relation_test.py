import psycopg2
import psycopg2.extras

import helper as t

psycopg2.extras.register_hstore(psycopg2.connect(**t.db_conf), globally=True)

mapping_file = 'route_relation_mapping.json'

def setup():
    t.setup()

def teardown():
    t.teardown()

RELOFFSET = int(-1e17)

#######################################################################
def test_import():
    """Import succeeds"""
    t.drop_schemas()
    assert not t.table_exists('osm_routes', schema=t.TEST_SCHEMA_IMPORT)
    t.imposm3_import(t.db_conf, './build/route_relation.pbf', mapping_file)
    assert t.table_exists('osm_routes', schema=t.TEST_SCHEMA_IMPORT)

def test_deploy():
    """Deploy succeeds"""
    assert not t.table_exists('osm_routes', schema=t.TEST_SCHEMA_PRODUCTION)
    t.imposm3_deploy(t.db_conf, mapping_file)
    assert t.table_exists('osm_routes', schema=t.TEST_SCHEMA_PRODUCTION)
    assert not t.table_exists('osm_routes', schema=t.TEST_SCHEMA_IMPORT)

#######################################################################


#######################################################################

def test_update():
    """Diff import applies"""
    t.imposm3_update(t.db_conf, './build/route_relation.osc.gz', mapping_file)

#######################################################################


#######################################################################
def test_deploy_and_revert_deploy():
    """Revert deploy succeeds"""
    assert not t.table_exists('osm_routes', schema=t.TEST_SCHEMA_IMPORT)
    assert t.table_exists('osm_routes', schema=t.TEST_SCHEMA_PRODUCTION)
    assert not t.table_exists('osm_routes', schema=t.TEST_SCHEMA_BACKUP)

    # import again to have a new import schema
    t.imposm3_import(t.db_conf, './build/route_relation.pbf', mapping_file)
    assert t.table_exists('osm_routes', schema=t.TEST_SCHEMA_IMPORT)

    t.imposm3_deploy(t.db_conf, mapping_file)
    assert not t.table_exists('osm_routes', schema=t.TEST_SCHEMA_IMPORT)
    assert t.table_exists('osm_routes', schema=t.TEST_SCHEMA_PRODUCTION)
    assert t.table_exists('osm_routes', schema=t.TEST_SCHEMA_BACKUP)

    t.imposm3_revert_deploy(t.db_conf, mapping_file)
    assert t.table_exists('osm_routes', schema=t.TEST_SCHEMA_IMPORT)
    assert t.table_exists('osm_routes', schema=t.TEST_SCHEMA_PRODUCTION)
    assert not t.table_exists('osm_routes', schema=t.TEST_SCHEMA_BACKUP)

def test_remove_backup():
    """Remove backup succeeds"""
    assert t.table_exists('osm_routes', schema=t.TEST_SCHEMA_IMPORT)
    assert t.table_exists('osm_routes', schema=t.TEST_SCHEMA_PRODUCTION)
    assert not t.table_exists('osm_routes', schema=t.TEST_SCHEMA_BACKUP)

    t.imposm3_deploy(t.db_conf, mapping_file)

    assert not t.table_exists('osm_routes', schema=t.TEST_SCHEMA_IMPORT)
    assert t.table_exists('osm_routes', schema=t.TEST_SCHEMA_PRODUCTION)
    assert t.table_exists('osm_routes', schema=t.TEST_SCHEMA_BACKUP)

    t.imposm3_remove_backups(t.db_conf, mapping_file)

    assert not t.table_exists('osm_routes', schema=t.TEST_SCHEMA_IMPORT)
    assert t.table_exists('osm_routes', schema=t.TEST_SCHEMA_PRODUCTION)
    assert not t.table_exists('osm_routes', schema=t.TEST_SCHEMA_BACKUP)

