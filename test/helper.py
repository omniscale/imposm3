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

__all__ = [
    "assert_almost_equal",
    "query_row",
    "cache_query",
    "merc_point",
    "imposm3_import",
    "imposm3_deploy",
    "imposm3_update",
    "imposm3_revert_deploy",
    "imposm3_remove_backups",
    "table_exists",
    "drop_schemas",
    "TEST_SCHEMA_IMPORT",
    "TEST_SCHEMA_PRODUCTION",
    "TEST_SCHEMA_BACKUP",
    "db_conf",
    "assert_missing_node",
    "assert_cached_node",
    "assert_cached_way",
]

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
    drop_schemas()
    _close_test_connection(db_conf)


db_conf = {
    'host': 'localhost',
}

TEST_SCHEMA_IMPORT = "imposm3testimport"
TEST_SCHEMA_PRODUCTION = "imposm3testpublic"
TEST_SCHEMA_BACKUP = "imposm3testbackup"

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
    conn = _test_connection(db_conf)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute('select * from %s.%s where osm_id = %%s' % (TEST_SCHEMA_PRODUCTION, table), [osmid])
    results = []
    for row in cur.fetchall():
        create_geom_in_row(row)
        results.append(row)
    cur.close()

    if not results:
        return None
    if len(results) == 1:
        return results[0]
    return results

def imposm3_import(db_conf, pbf, mapping_file):
    _close_test_connection(db_conf)
    conn = pg_db_url(db_conf)

    try:
        print subprocess.check_output((
            "../imposm3 import -connection %s -read %s"
            " -write"
            " -cachedir %s"
            " -diff"
            " -overwritecache"
            " -dbschema-import " + TEST_SCHEMA_IMPORT +
            " -optimize"
            " -mapping %s ") % (
            conn, pbf, tmpdir, mapping_file
        ), shell=True)
    except subprocess.CalledProcessError, ex:
        print ex.output
        raise

def imposm3_deploy(db_conf, mapping_file):
    _close_test_connection(db_conf)
    conn = pg_db_url(db_conf)

    try:
        print subprocess.check_output((
            "../imposm3 import -connection %s"
            " -dbschema-import " + TEST_SCHEMA_IMPORT +
            " -dbschema-production " + TEST_SCHEMA_PRODUCTION +
            " -dbschema-backup " + TEST_SCHEMA_BACKUP +
            " -deployproduction"
            " -mapping %s ") % (
            conn, mapping_file,
        ), shell=True)
    except subprocess.CalledProcessError, ex:
        print ex.output
        raise

def imposm3_revert_deploy(db_conf, mapping_file):
    _close_test_connection(db_conf)
    conn = pg_db_url(db_conf)

    try:
        print subprocess.check_output((
            "../imposm3 import -connection %s"
            " -dbschema-import " + TEST_SCHEMA_IMPORT +
            " -dbschema-production " + TEST_SCHEMA_PRODUCTION +
            " -dbschema-backup " + TEST_SCHEMA_BACKUP +
            " -revertdeploy"
            " -mapping %s ") % (
            conn, mapping_file,
        ), shell=True)
    except subprocess.CalledProcessError, ex:
        print ex.output
        raise

def imposm3_remove_backups(db_conf, mapping_file):
    _close_test_connection(db_conf)
    conn = pg_db_url(db_conf)

    try:
        print subprocess.check_output((
            "../imposm3 import -connection %s"
            " -dbschema-backup " + TEST_SCHEMA_BACKUP +
            " -removebackup"
            " -mapping %s ") % (
            conn, mapping_file,
        ), shell=True)
    except subprocess.CalledProcessError, ex:
        print ex.output
        raise

def imposm3_update(db_conf, osc, mapping_file):
    _close_test_connection(db_conf)
    conn = pg_db_url(db_conf)

    try:
        print subprocess.check_output((
            "../imposm3 diff -connection %s"
            " -cachedir %s"
            " -limitto clipping-3857.geojson"
            " -dbschema-production " + TEST_SCHEMA_PRODUCTION +
            " -mapping %s %s") % (
            conn, tmpdir, mapping_file, osc,
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

def _test_connection(db_conf):
    if '_connection' in db_conf:
        return db_conf['_connection']
    db_conf['_connection'] = psycopg2.connect(**db_conf)
    return db_conf['_connection']

def _close_test_connection(db_conf):
    if '_connection' in db_conf:
        db_conf['_connection'].close()
        del db_conf['_connection']

def table_exists(table, schema=TEST_SCHEMA_IMPORT):
    conn = _test_connection(db_conf)
    cur = conn.cursor()
    cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='%s' AND table_schema='%s')"
        % (table, schema))

    exists = cur.fetchone()[0]
    cur.close()
    return exists

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

def drop_schemas():
    conn = _test_connection(db_conf)
    cur = conn.cursor()
    cur.execute("DROP SCHEMA IF EXISTS %s CASCADE" % TEST_SCHEMA_IMPORT)
    cur.execute("DROP SCHEMA IF EXISTS %s CASCADE" % TEST_SCHEMA_PRODUCTION)
    cur.execute("DROP SCHEMA IF EXISTS %s CASCADE" % TEST_SCHEMA_BACKUP)
    conn.commit()

