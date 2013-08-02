import math
import tempfile
import atexit
import shutil
import subprocess
import psycopg2
import psycopg2.extras
import json
from  shapely.wkb import loads as wkb_loads
from shapely.geometry import Point
import binascii

import unittest

class Dummy(unittest.TestCase):
    def nop():
        pass
_t = Dummy('nop')
assert_almost_equal = _t.assertAlmostEqual

# tmpdir = tempfile.mkdtemp()

# def cleanup_tmpdir():
#     shutil.rmtree(tmpdir)

# atexit.register(cleanup_tmpdir)

tmpdir = '/tmp/testtest'

db_conf = {
    'database': 'olt',
    'user': 'olt',
    'password': 'olt',
    'host': 'localhost',
}

def merc_point(lon, lat):
    pole = 6378137 * math.pi # 20037508.342789244

    x = lon * pole / 180.0
    y = math.log(math.tan((90.0+lat)*math.pi/360.0)) / math.pi * pole
    return Point(x, y)


def pg_db_url(db_conf):
    return 'postgis://%(user)s:%(password)s@%(host)s/%(database)s' % db_conf

def create_geom_in_row(rowdict):
    if rowdict:
        rowdict['geometry'] = wkb_loads(binascii.unhexlify(rowdict['geometry']))
    return rowdict

def query_row(db_conf, table, osmid):
    conn = psycopg2.connect(**db_conf)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute('select * from import.%s where osm_id = %%s' % table, [osmid])
    row = cur.fetchone()
    create_geom_in_row(row)
    return row

def goposm_import(db_conf, pbf):
    conn = pg_db_url(db_conf)

    try:
        print subprocess.check_output(
            "../goposm import -connection %s -read %s"
            " -write"
            " -cachedir %s"
            " -diff"
            " -overwritecache"
            " -mapping test_mapping.json " % (
            conn, pbf, tmpdir,
        ), shell=True)
    except subprocess.CalledProcessError, ex:
        print ex.output
        raise

def goposm_update(db_conf, osc):
    conn = pg_db_url(db_conf)

    try:
        print subprocess.check_output(
            "../goposm diff -connection %s"
            " -cachedir %s"
            " -mapping test_mapping.json %s" % (
            conn, tmpdir, osc,
        ), shell=True)
    except subprocess.CalledProcessError, ex:
        print ex.output
        raise

def cache_query(nodes='', ways='', relations=''):
    if nodes:
        nodes = '-node ' + ','.join(map(str, nodes))
    if ways:
        ways = '-way ' + ','.join(map(str, ways))
    if relations:
        relations = '-rel' + ','.join(map(str, relations))
    out = subprocess.check_output(
        "../goposm query-cache -cachedir %s %s %s %s" % (tmpdir, nodes, ways, relations),
        shell=True)
    print out
    return json.loads(out)

def table_exists(table):
    conn = psycopg2.connect(**db_conf)
    cur = conn.cursor()
    cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='%s' AND table_schema='%s')"
        % (table, 'import'))
    return cur.fetchone()[0]

def assert_missing_node(id):
    data = cache_query(nodes=[id])
    if data['nodes'][str(id)]:
        raise AssertionError('node %d found' % id)

def assert_cached_node(id, (lon, lat)=(None, None)):
    data = cache_query(nodes=[id])
    node = data['nodes'][str(id)]
    if not node:
        raise AssertionError('node %d not found' % id)

    if lon and lat:
        assert_almost_equal(lon, node['lon'], 6)
        assert_almost_equal(lat, node['lat'], 6)

def assert_cached_way(id):
    data = cache_query(ways=[id])
    if not data['ways'][str(id)]:
        raise AssertionError('way %d not found' % id)

def drop_import_schema():
    conn = psycopg2.connect(**db_conf)
    cur = conn.cursor()
    cur.execute("DROP SCHEMA IF EXISTS import CASCADE")
    conn.commit()

#######################################################################
def test_import():
    """Import succeeds"""
    drop_import_schema()
    assert not table_exists('osm_roads')
    goposm_import(db_conf, './test.pbf')
    assert table_exists('osm_roads')
#######################################################################

def test_imported_landusage():
    """Multipolygon relation is inserted"""
    assert_cached_node(1001, (13, 47.5))
    landusage_1001 = query_row(db_conf, 'osm_landusages', 1001)
    # point in polygon
    assert landusage_1001['geometry'].intersects(merc_point(13.4, 47.5))
    # hole in multipolygon relation
    assert not landusage_1001['geometry'].intersects(merc_point(14.75, 47.75))

def test_missing_nodes():
    """Cache does not contain nodes from previous imports"""
    assert_missing_node(10001)
    assert_missing_node(10002)
    place_10000 = query_row(db_conf, 'osm_places', 10000)
    assert place_10000['name'] == 'Foo', place_10000


def test_landusage_to_waterarea_1():
    """Parks inserted into landusages"""
    assert_cached_way(11001)
    assert_cached_way(12001)
    assert_cached_way(13001)

    assert not query_row(db_conf, 'osm_waterareas', 11001)
    assert not query_row(db_conf, 'osm_waterareas', 12001)
    assert not query_row(db_conf, 'osm_waterareas', 13001)

    assert query_row(db_conf, 'osm_landusages', 11001)['type'] == 'park'
    assert query_row(db_conf, 'osm_landusages', 12001)['type'] == 'park'
    assert query_row(db_conf, 'osm_landusages', 13001)['type'] == 'park'


def test_changed_hole_tags_1():
    """Multipolygon relation with untagged hole"""
    assert_cached_way(14001)
    assert_cached_way(14011)

    assert not query_row(db_conf, 'osm_waterareas', 14011)
    assert query_row(db_conf, 'osm_landusages', 14001)['type'] == 'park'

#######################################################################
def test_update():
    """Diff import applies"""
    goposm_update(db_conf, './test.osc.gz')
#######################################################################

def test_updated_landusage():
    """Multipolygon relation was modified"""
    assert_cached_node(1001, (13.5, 47.5))
    landusage_1001 = query_row(db_conf, 'osm_landusages', 1001)
    # point not in polygon after update
    assert not landusage_1001['geometry'].intersects(merc_point(13.4, 47.5))

def test_partial_delete():
    """Deleted relation but nodes are still cached"""
    assert_cached_node(2001)
    assert_cached_way(2001)
    assert_cached_way(2002)
    assert not query_row(db_conf, 'osm_landusages', 2001)

def test_updated_nodes():
    """Nodes were added, modified or deleted"""
    assert_missing_node(10000)
    assert_cached_node(10001, (10.0, 40.0))
    assert_cached_node(10002, (10.1, 40.0))
    place_10001 = query_row(db_conf, 'osm_places', 10001)
    assert place_10001['name'] == 'Bar', place_10001
    place_10001 = query_row(db_conf, 'osm_places', 10002)
    assert place_10001['name'] == 'Baz', place_10001

def test_landusage_to_waterarea_2():
    """Parks converted to water moved from landusages to waterareas"""
    assert_cached_way(11001)
    assert_cached_way(12001)
    assert_cached_way(13001)

    assert not query_row(db_conf, 'osm_landusages', 11001)
    assert not query_row(db_conf, 'osm_landusages', 12001)
    assert not query_row(db_conf, 'osm_landusages', 13001)

    assert query_row(db_conf, 'osm_waterareas', 11001)['type'] == 'water'
    assert query_row(db_conf, 'osm_waterareas', 12001)['type'] == 'water'
    assert query_row(db_conf, 'osm_waterareas', 13001)['type'] == 'water'


def test_changed_hole_tags_2():
    """Newly tagged hole is inserted"""
    assert_cached_way(14001)
    assert_cached_way(14011)

    assert query_row(db_conf, 'osm_waterareas', 14011)['type'] == 'water'
    assert query_row(db_conf, 'osm_landusages', 14001)['type'] == 'park'
    assert_almost_equal(query_row(db_conf, 'osm_landusages', 14001)['geometry'].area, 10373600000, -6)
    assert_almost_equal(query_row(db_conf, 'osm_waterareas', 14011)['geometry'].area, 26672000000, -6)

