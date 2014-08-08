import psycopg2
import psycopg2.extras

import helper as t

psycopg2.extras.register_hstore(psycopg2.connect(**t.db_conf), globally=True)

mapping_file = 'single_table_mapping.json'

def setup():
    t.setup()

def teardown():
    t.teardown()

#######################################################################
def test_import():
    """Import succeeds"""
    t.drop_schemas()
    assert not t.table_exists('osm_all', schema=t.TEST_SCHEMA_IMPORT)
    t.imposm3_import(t.db_conf, './build/single_table.pbf', mapping_file)
    assert t.table_exists('osm_all', schema=t.TEST_SCHEMA_IMPORT)

def test_deploy():
    """Deploy succeeds"""
    assert not t.table_exists('osm_all', schema=t.TEST_SCHEMA_PRODUCTION)
    t.imposm3_deploy(t.db_conf, mapping_file)
    assert t.table_exists('osm_all', schema=t.TEST_SCHEMA_PRODUCTION)
    assert not t.table_exists('osm_all', schema=t.TEST_SCHEMA_IMPORT)

#######################################################################

def test_non_mapped_node_is_missing():
    """Node without mapped tags is missing."""
    t.assert_cached_node(10001, (10, 42))
    assert not t.query_row(t.db_conf, 'osm_all', 10001)

def test_mapped_node():
    """Node is stored with all tags."""
    t.assert_cached_node(10002, (11, 42))
    poi = t.query_row(t.db_conf, 'osm_all', 10002)
    assert poi['tags'] == {'random': 'tag', 'but': 'mapped', 'poi': 'unicorn'}


def test_non_mapped_way_is_missing():
    """Way without mapped tags is missing."""
    t.assert_cached_way(20101)
    assert not t.query_row(t.db_conf, 'osm_all', 20101)
    t.assert_cached_way(20102)
    assert not t.query_row(t.db_conf, 'osm_all', 20102)
    t.assert_cached_way(20103)
    assert not t.query_row(t.db_conf, 'osm_all', 20103)

def test_mapped_way():
    """Way is stored with all tags."""
    t.assert_cached_way(20201)
    highway = t.query_row(t.db_conf, 'osm_all', 20201)
    assert highway['tags'] == {'random': 'tag', 'highway': 'yes'}

def test_non_mapped_closed_way_is_missing():
    """Closed way without mapped tags is missing."""
    t.assert_cached_way(20301)
    assert not t.query_row(t.db_conf, 'osm_all', 20301)

def test_mapped_closed_way():
    """Closed way is stored with all tags."""
    t.assert_cached_way(20401)
    building = t.query_row(t.db_conf, 'osm_all', 20401)
    assert building['tags'] == {'random': 'tag', 'building': 'yes'}

def test_mapped_closed_way_area_yes():
    """Closed way with area=yes is not stored as linestring."""
    t.assert_cached_way(20501)
    elem = t.query_row(t.db_conf, 'osm_all', 20501)
    assert elem['geometry'].type == 'Polygon', elem['geometry'].type
    assert elem['tags'] == {'random': 'tag', 'landuse': 'grass', 'highway': 'pedestrian', 'area': 'yes'}

def test_mapped_closed_way_area_no():
    """Closed way with area=no is not stored as polygon."""
    t.assert_cached_way(20502)
    elem = t.query_row(t.db_conf, 'osm_all', 20502)
    assert elem['geometry'].type == 'LineString', elem['geometry'].type
    assert elem['tags'] == {'random': 'tag', 'landuse': 'grass', 'highway': 'pedestrian', 'area': 'no'}

def test_mapped_closed_way_without_area():
    """Closed way without area is stored as mapped (linestring and polygon)."""
    t.assert_cached_way(20601)
    elems = t.query_row(t.db_conf, 'osm_all', 20601)
    assert len(elems) == 2
    elems.sort(key=lambda x: x['geometry'].type)

    assert elems[0]['geometry'].type == 'LineString', elem['geometry'].type
    assert elems[0]['tags'] == {'random': 'tag', 'landuse': 'grass', 'highway': 'pedestrian'}
    assert elems[1]['geometry'].type == 'Polygon', elem['geometry'].type
    assert elems[1]['tags'] == {'random': 'tag', 'landuse': 'grass', 'highway': 'pedestrian'}


#######################################################################
def test_deploy_and_revert_deploy():
    """Revert deploy succeeds"""
    assert not t.table_exists('osm_all', schema=t.TEST_SCHEMA_IMPORT)
    assert t.table_exists('osm_all', schema=t.TEST_SCHEMA_PRODUCTION)
    assert not t.table_exists('osm_all', schema=t.TEST_SCHEMA_BACKUP)

    # import again to have a new import schema
    t.imposm3_import(t.db_conf, './build/single_table.pbf', mapping_file)
    assert t.table_exists('osm_all', schema=t.TEST_SCHEMA_IMPORT)

    t.imposm3_deploy(t.db_conf, mapping_file)
    assert not t.table_exists('osm_all', schema=t.TEST_SCHEMA_IMPORT)
    assert t.table_exists('osm_all', schema=t.TEST_SCHEMA_PRODUCTION)
    assert t.table_exists('osm_all', schema=t.TEST_SCHEMA_BACKUP)

    t.imposm3_revert_deploy(t.db_conf, mapping_file)
    assert t.table_exists('osm_all', schema=t.TEST_SCHEMA_IMPORT)
    assert t.table_exists('osm_all', schema=t.TEST_SCHEMA_PRODUCTION)
    assert not t.table_exists('osm_all', schema=t.TEST_SCHEMA_BACKUP)

def test_remove_backup():
    """Remove backup succeeds"""
    assert t.table_exists('osm_all', schema=t.TEST_SCHEMA_IMPORT)
    assert t.table_exists('osm_all', schema=t.TEST_SCHEMA_PRODUCTION)
    assert not t.table_exists('osm_all', schema=t.TEST_SCHEMA_BACKUP)

    t.imposm3_deploy(t.db_conf, mapping_file)

    assert not t.table_exists('osm_all', schema=t.TEST_SCHEMA_IMPORT)
    assert t.table_exists('osm_all', schema=t.TEST_SCHEMA_PRODUCTION)
    assert t.table_exists('osm_all', schema=t.TEST_SCHEMA_BACKUP)

    t.imposm3_remove_backups(t.db_conf, mapping_file)

    assert not t.table_exists('osm_all', schema=t.TEST_SCHEMA_IMPORT)
    assert t.table_exists('osm_all', schema=t.TEST_SCHEMA_PRODUCTION)
    assert not t.table_exists('osm_all', schema=t.TEST_SCHEMA_BACKUP)

