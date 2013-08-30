import math
import tempfile
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

tmpdir = None

def setup():
    global tmpdir
    tmpdir = tempfile.mkdtemp()

def teardown():
    shutil.rmtree(tmpdir)


db_conf = {
    'host': 'localhost',
}

def merc_point(lon, lat):
    pole = 6378137 * math.pi # 20037508.342789244

    x = lon * pole / 180.0
    y = math.log(math.tan((90.0+lat)*math.pi/360.0)) / math.pi * pole
    return Point(x, y)


def pg_db_url(db_conf):
    return 'postgis://%(host)s' % db_conf

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

def imposm3_import(db_conf, pbf):
    conn = pg_db_url(db_conf)

    try:
        print subprocess.check_output(
            "../imposm3 import -connection %s -read %s"
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

def imposm3_update(db_conf, osc):
    conn = pg_db_url(db_conf)

    try:
        print subprocess.check_output(
            "../imposm3 diff -connection %s"
            " -cachedir %s"
            " -limitto clipping-3857.geojson"
            " -mapping test_mapping.json %s" % (
            conn, tmpdir, osc,
        ), shell=True)
    except subprocess.CalledProcessError, ex:
        print ex.output
        raise

def cache_query(nodes='', ways='', relations='', deps='', full=''):
    if nodes:
        nodes = '-node ' + ','.join(map(str, nodes))
    if ways:
        ways = '-way ' + ','.join(map(str, ways))
    if relations:
        relations = '-rel ' + ','.join(map(str, relations))
    if deps:
        deps = '-deps'
    if full:
        full = '-full'
    out = subprocess.check_output(
        "../imposm3 query-cache -cachedir %s %s %s %s %s %s" % (
            tmpdir, nodes, ways, relations, deps, full),
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
    imposm3_import(db_conf, './build/test.pbf')
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

def test_split_outer_multipolygon_way_1():
    """Single outer way of multipolygon was inserted."""
    park_15001 = query_row(db_conf, 'osm_landusages', 15001)
    assert park_15001['type'] == 'park'
    assert_almost_equal(park_15001['geometry'].area, 9816216452, -1)
    assert query_row(db_conf, 'osm_roads', 15002) == None

def test_merge_outer_multipolygon_way_1():
    """Splitted outer way of multipolygon was inserted."""
    park_16001 = query_row(db_conf, 'osm_landusages', 16001)
    assert park_16001['type'] == 'park'
    assert_almost_equal(park_16001['geometry'].area, 12779350582, -1)
    assert query_row(db_conf, 'osm_roads', 16002)['type'] == 'residential'

def test_node_way_ref_after_delete_1():
    """Nodes refereces way"""
    data = cache_query(nodes=[20001, 20002], deps=True)
    assert '20001' in data['nodes']['20001']['ways']
    assert '20001' in data['nodes']['20002']['ways']
    assert query_row(db_conf, 'osm_roads', 20001)['type'] == 'residential'
    assert query_row(db_conf, 'osm_barrierpoints', 20001)['type'] == 'block'

def test_way_rel_ref_after_delete_1():
    """Ways refereces relation"""
    data = cache_query(ways=[21001], deps=True)
    assert data['ways']['21001']['relations'].keys() == ['21001']
    assert query_row(db_conf, 'osm_roads', 21001)['type'] == 'residential'
    assert query_row(db_conf, 'osm_landusages', 21001)['type'] == 'park'


#######################################################################
def test_update():
    """Diff import applies"""
    imposm3_update(db_conf, './build/test.osc.gz')
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
    place_10002 = query_row(db_conf, 'osm_places', 10002)
    assert place_10002['name'] == 'Baz', place_10002

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

def test_split_outer_multipolygon_way_2():
    """Splitted outer way of multipolygon was inserted"""
    data = cache_query(ways=[15001, 15002], deps=True)
    assert data['ways']['15001']['relations'].keys() == ['15001']
    assert data['ways']['15002']['relations'].keys() == ['15001']

    park_15001 = query_row(db_conf, 'osm_landusages', 15001)
    assert park_15001['type'] == 'park'
    assert_almost_equal(park_15001['geometry'].area, 9816216452, -1)
    assert query_row(db_conf, 'osm_roads', 15002)['type'] == 'residential'

def test_merge_outer_multipolygon_way_2():
    """Merged outer way of multipolygon was inserted"""
    data = cache_query(ways=[16001, 16002], deps=True)
    assert data['ways']['16001']['relations'].keys() == ['16001']
    assert data['ways']['16002'] == None

    data = cache_query(relations=[16001], full=True)
    assert sorted(data['relations']['16001']['ways'].keys()) == ['16001', '16011']

    park_16001 = query_row(db_conf, 'osm_landusages', 16001)
    assert park_16001['type'] == 'park'
    assert_almost_equal(park_16001['geometry'].area, 12779350582, -1)
    assert query_row(db_conf, 'osm_roads', 16002) == None


def test_node_way_ref_after_delete_2():
    """Node does not referece deleted way"""
    data = cache_query(nodes=[20001, 20002], deps=True)
    assert 'ways' not in data['nodes']['20001']
    assert data['nodes']['20002'] == None
    assert query_row(db_conf, 'osm_roads', 20001) == None
    assert query_row(db_conf, 'osm_barrierpoints', 20001)['type'] == 'block'


def test_way_rel_ref_after_delete_2():
    """Way does not referece deleted relation"""
    data = cache_query(ways=[21001], deps=True)
    assert 'relations' not in data['ways']['21001']
    assert query_row(db_conf, 'osm_roads', 21001)['type'] == 'residential'
    assert query_row(db_conf, 'osm_landusages', 21001) == None

