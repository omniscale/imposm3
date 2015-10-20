import unittest

import helper as t

mapping_file = 'complete_db_mapping.json'

def setup():
    t.setup()

def teardown():
    t.teardown()

#######################################################################
def test_import():
    """Import succeeds"""
    t.drop_schemas()
    assert not t.table_exists('osm_roads', schema=t.TEST_SCHEMA_IMPORT)
    t.imposm3_import(t.db_conf, './build/complete_db.pbf', mapping_file)
    assert t.table_exists('osm_roads', schema=t.TEST_SCHEMA_IMPORT)

def test_deploy():
    """Deploy succeeds"""
    assert not t.table_exists('osm_roads', schema=t.TEST_SCHEMA_PRODUCTION)
    t.imposm3_deploy(t.db_conf, mapping_file)
    assert t.table_exists('osm_roads', schema=t.TEST_SCHEMA_PRODUCTION)
    assert not t.table_exists('osm_roads', schema=t.TEST_SCHEMA_IMPORT)

#######################################################################

def test_imported_landusage():
    """Multipolygon relation is inserted"""
    t.assert_cached_node(1001, (13, 47.5))
    landusage_1001 = t.query_row(t.db_conf, 'osm_landusages', -1001)
    # point in polygon
    assert landusage_1001['geometry'].intersects(t.merc_point(13.4, 47.5))
    # hole in multipolygon relation
    assert not landusage_1001['geometry'].intersects(t.merc_point(14.75, 47.75))

def test_missing_nodes():
    """Cache does not contain nodes from previous imports"""
    t.assert_missing_node(10001)
    t.assert_missing_node(10002)
    place_10000 = t.query_row(t.db_conf, 'osm_places', 10000)
    assert place_10000['name'] == 'Foo', place_10000

def test_name_tags():
    """Road contains multiple names"""
    road = t.query_row(t.db_conf, 'osm_roads', 1101)
    assert road['name'] == 'name', road
    assert road['name:de'] == 'name:de', road
    assert road['name_en'] == 'name:en', road

def test_landusage_to_waterarea_1():
    """Parks inserted into landusages"""
    t.assert_cached_way(11001)
    t.assert_cached_way(12001)
    t.assert_cached_way(13001)

    assert not t.query_row(t.db_conf, 'osm_waterareas', 11001)
    assert not t.query_row(t.db_conf, 'osm_waterareas', -12001)
    assert not t.query_row(t.db_conf, 'osm_waterareas', -13001)

    assert not t.query_row(t.db_conf, 'osm_waterareas_gen0', 11001)
    assert not t.query_row(t.db_conf, 'osm_waterareas_gen0', -12001)
    assert not t.query_row(t.db_conf, 'osm_waterareas_gen0', -13001)

    assert not t.query_row(t.db_conf, 'osm_waterareas_gen1', 11001)
    assert not t.query_row(t.db_conf, 'osm_waterareas_gen1', -12001)
    assert not t.query_row(t.db_conf, 'osm_waterareas_gen1', -13001)

    assert t.query_row(t.db_conf, 'osm_landusages', 11001)['type'] == 'park'
    assert t.query_row(t.db_conf, 'osm_landusages', -12001)['type'] == 'park'
    assert t.query_row(t.db_conf, 'osm_landusages', -13001)['type'] == 'park'

    assert t.query_row(t.db_conf, 'osm_landusages_gen0', 11001)['type'] == 'park'
    assert t.query_row(t.db_conf, 'osm_landusages_gen0', -12001)['type'] == 'park'
    assert t.query_row(t.db_conf, 'osm_landusages_gen0', -13001)['type'] == 'park'

    assert t.query_row(t.db_conf, 'osm_landusages_gen1', 11001)['type'] == 'park'
    assert t.query_row(t.db_conf, 'osm_landusages_gen1', -12001)['type'] == 'park'
    assert t.query_row(t.db_conf, 'osm_landusages_gen1', -13001)['type'] == 'park'


def test_changed_hole_tags_1():
    """Multipolygon relation with untagged hole"""
    t.assert_cached_way(14001)
    t.assert_cached_way(14011)

    assert not t.query_row(t.db_conf, 'osm_waterareas', 14011)
    assert not t.query_row(t.db_conf, 'osm_waterareas', -14011)
    assert t.query_row(t.db_conf, 'osm_landusages', -14001)['type'] == 'park'

def test_split_outer_multipolygon_way_1():
    """Single outer way of multipolygon was inserted."""
    park_15001 = t.query_row(t.db_conf, 'osm_landusages', -15001)
    assert park_15001['type'] == 'park'
    t.assert_almost_equal(park_15001['geometry'].area, 9816216452, -1)
    assert t.query_row(t.db_conf, 'osm_roads', 15002) == None

def test_merge_outer_multipolygon_way_1():
    """Splitted outer way of multipolygon was inserted."""
    park_16001 = t.query_row(t.db_conf, 'osm_landusages', -16001)
    assert park_16001['type'] == 'park'
    t.assert_almost_equal(park_16001['geometry'].area, 12779350582, -1)
    assert t.query_row(t.db_conf, 'osm_roads', 16002)['type'] == 'residential'

def test_broken_multipolygon_ways():
    """MultiPolygons with broken outer ways are handled."""
    # outer way does not merge (17002 has one node)
    assert t.query_row(t.db_conf, 'osm_landusages', -17001) == None
    assert t.query_row(t.db_conf, 'osm_roads', 17001)['type'] == 'residential'
    assert t.query_row(t.db_conf, 'osm_roads', 17002) == None

    # outer way does not merge (17102 has no nodes)
    assert t.query_row(t.db_conf, 'osm_landusages', -17101) == None
    assert t.query_row(t.db_conf, 'osm_roads', 17101)['type'] == 'residential'
    assert t.query_row(t.db_conf, 'osm_roads', 17102) == None

def test_node_way_inserted_twice():
    """Way with multiple mappings is inserted twice in same table"""
    rows = t.query_row(t.db_conf, 'osm_roads', 18001)
    rows.sort(key=lambda x: x['type'])

    assert rows[0]['type'] == 'residential'
    assert rows[1]['type'] == 'tram'

def test_outer_way_not_inserted():
    """Outer way with different tag is not inserted twice into same table"""
    farm = t.query_row(t.db_conf, 'osm_landusages', -19001)
    assert farm['type'] == 'farmland'
    assert not t.query_row(t.db_conf, 'osm_landusages', 19001)

    farmyard = t.query_row(t.db_conf, 'osm_landusages', 19002)
    assert farmyard['type'] == 'farmyard'

def test_outer_way_inserted():
    """Outer way with different tag is inserted twice into different table"""
    farm = t.query_row(t.db_conf, 'osm_landusages', 19101)
    assert farm['type'] == 'farm'
    assert not t.query_row(t.db_conf, 'osm_landusages', -19101)

    farmyard = t.query_row(t.db_conf, 'osm_landusages', 19102)
    assert farmyard['type'] == 'farmyard'

    admin = t.query_row(t.db_conf, 'osm_admin', -19101)
    assert admin['type'] == 'administrative'

def test_node_way_ref_after_delete_1():
    """Nodes refereces way"""
    data = t.cache_query(nodes=[20001, 20002], deps=True)
    assert '20001' in data['nodes']['20001']['ways']
    assert '20001' in data['nodes']['20002']['ways']
    assert t.query_row(t.db_conf, 'osm_roads', 20001)['type'] == 'residential'
    assert t.query_row(t.db_conf, 'osm_barrierpoints', 20001)['type'] == 'block'

def test_way_rel_ref_after_delete_1():
    """Ways references relation"""
    data = t.cache_query(ways=[21001], deps=True)
    assert data['ways']['21001']['relations'].keys() == ['21001']
    assert t.query_row(t.db_conf, 'osm_roads', 21001)['type'] == 'residential'
    assert t.query_row(t.db_conf, 'osm_landusages', -21001)['type'] == 'park'

def test_relation_way_not_inserted():
    """Part of relation was inserted only once."""
    park = t.query_row(t.db_conf, 'osm_landusages', -9001)
    assert park['type'] == 'park'
    assert park['name'] == 'rel 9001'
    assert t.query_row(t.db_conf, 'osm_landusages', 9009) == None

    park = t.query_row(t.db_conf, 'osm_landusages', -9101)
    assert park['type'] == 'park'
    assert park['name'] == 'rel 9101'
    assert t.query_row(t.db_conf, 'osm_landusages', 9109) == None

    scrub = t.query_row(t.db_conf, 'osm_landusages', 9110)
    assert scrub['type'] == 'scrub'

def test_relation_ways_inserted():
    """Outer ways of multipolygon are inserted. """
    park = t.query_row(t.db_conf, 'osm_landusages', -9201)
    assert park['type'] == 'park'
    assert park['name'] == '9209'
    assert not t.query_row(t.db_conf, 'osm_landusages', 9201)

    # outer ways of multipolygon stand for their own
    road = t.query_row(t.db_conf, 'osm_roads', 9209)
    assert road['type'] == 'secondary'
    assert road['name'] == '9209'
    road = t.query_row(t.db_conf, 'osm_roads', 9210)
    assert road['type'] == 'residential'
    assert road['name'] == '9210'

    park = t.query_row(t.db_conf, 'osm_landusages', -9301)
    assert park['type'] == 'park'
    assert park['name'] == '' # no name on relation

    # outer ways of multipolygon stand for their own
    road = t.query_row(t.db_conf, 'osm_roads', 9309)
    assert road['type'] == 'secondary'
    assert road['name'] == '9309'
    road = t.query_row(t.db_conf, 'osm_roads', 9310)
    assert road['type'] == 'residential'
    assert road['name'] == '9310'

def test_relation_way_inserted():
    """Part of relation was inserted twice."""
    park = t.query_row(t.db_conf, 'osm_landusages', -8001)
    assert park['type'] == 'park'
    assert park['name'] == 'rel 8001'
    assert t.query_row(t.db_conf, 'osm_roads', 8009)["type"] == 'residential'

def test_single_node_ways_not_inserted():
    """Ways with single/duplicate nodes are not inserted."""
    assert not t.query_row(t.db_conf, 'osm_roads', 30001)
    assert not t.query_row(t.db_conf, 'osm_roads', 30002)
    assert not t.query_row(t.db_conf, 'osm_roads', 30003)

def test_polygon_with_duplicate_nodes_is_valid():
    """Polygon with duplicate nodes is valid."""
    geom = t.query_row(t.db_conf, 'osm_landusages', 30005)['geometry']
    assert geom.is_valid
    assert len(geom.exterior.coords) == 4

def test_incomplete_polygons():
    """Non-closed/incomplete polygons are not inserted."""
    assert not t.query_row(t.db_conf, 'osm_landusages', 30004)
    assert not t.query_row(t.db_conf, 'osm_landusages', 30006)

def test_residential_to_secondary():
    """Residential road is not in roads_gen0/1."""
    assert t.query_row(t.db_conf, 'osm_roads', 40001)['type'] == 'residential'
    assert not t.query_row(t.db_conf, 'osm_roads_gen0', 40001)
    assert not t.query_row(t.db_conf, 'osm_roads_gen1', 40001)

def test_relation_before_remove():
    """Relation and way is inserted."""
    assert t.query_row(t.db_conf, 'osm_buildings', 50011)['type'] == 'yes'
    assert t.query_row(t.db_conf, 'osm_landusages', -50021)['type'] == 'park'

def test_relation_without_tags():
    """Relation without tags is inserted."""
    assert t.query_row(t.db_conf, 'osm_buildings', 50111) == None
    assert t.query_row(t.db_conf, 'osm_buildings', -50121)['type'] == 'yes'

def test_duplicate_ids():
    """Relation/way with same ID is inserted."""
    assert t.query_row(t.db_conf, 'osm_buildings', 51001)['type'] == 'way'
    assert t.query_row(t.db_conf, 'osm_buildings', -51001)['type'] == 'mp'
    assert t.query_row(t.db_conf, 'osm_buildings', 51011)['type'] == 'way'
    assert t.query_row(t.db_conf, 'osm_buildings', -51011)['type'] == 'mp'

def test_generalized_banana_polygon_is_valid():
    """Generalized polygons are valid."""
    park = t.query_row(t.db_conf, 'osm_landusages', 7101)
    # geometry is not valid
    assert not park['geometry'].is_valid, park
    park = t.query_row(t.db_conf, 'osm_landusages_gen0', 7101)
    # but simplified geometies are valid
    assert park['geometry'].is_valid, park
    park = t.query_row(t.db_conf, 'osm_landusages_gen1', 7101)
    assert park['geometry'].is_valid, park

def test_generalized_linestring_is_valid():
    """Generalized linestring is valid."""
    road = t.query_row(t.db_conf, 'osm_roads', 7201)
    # geometry is not simple, but valid
    # check that geometry 'survives' simplification
    assert not road['geometry'].is_simple, road['geometry'].wkt
    assert road['geometry'].is_valid, road['geometry'].wkt
    assert road['geometry'].length > 1000000
    road = t.query_row(t.db_conf, 'osm_roads_gen0', 7201)
    # but simplified geometies are simple
    assert road['geometry'].is_valid, road['geometry'].wkt
    assert road['geometry'].length > 1000000
    road = t.query_row(t.db_conf, 'osm_roads_gen1', 7201)
    assert road['geometry'].is_valid, road['geometry'].wkt
    assert road['geometry'].length > 1000000

def test_ring_with_gap():
    """Multipolygon and way with gap (overlapping but different endpoints) gets closed"""
    park = t.query_row(t.db_conf, 'osm_landusages', -7301)
    assert park['geometry'].is_valid, park

    park = t.query_row(t.db_conf, 'osm_landusages', 7311)
    assert park['geometry'].is_valid, park

def test_multipolygon_with_open_ring():
    """Multipolygon is inserted even if there is an open ring/member"""
    park = t.query_row(t.db_conf, 'osm_landusages', -7401)
    assert park['geometry'].is_valid, park

def test_updated_nodes1():
    """Zig-Zag line is inserted."""
    road =  t.query_row(t.db_conf, 'osm_roads', 60000)
    t.assert_almost_equal(road['geometry'].length, 14035.61150207768)

def test_update_node_to_coord_1():
    """Node is inserted with tag."""
    coords = t.cache_query(nodes=(70001, 70002))
    assert coords['nodes']["70001"]["tags"] == {"amenity": "police"}
    assert "tags" not in coords['nodes']["70002"]

    assert t.query_row(t.db_conf, 'osm_amenities', 70001)
    assert not t.query_row(t.db_conf, 'osm_amenities', 70002)

def test_enumerate_key():
    """Enumerate from key."""
    assert t.query_row(t.db_conf, 'osm_landusages', 100001)['enum'] == 1
    assert t.query_row(t.db_conf, 'osm_landusages', 100002)['enum'] == 0
    assert t.query_row(t.db_conf, 'osm_landusages', 100003)['enum'] == 15


#######################################################################
def test_update():
    """Diff import applies"""
    t.imposm3_update(t.db_conf, './build/complete_db.osc.gz', mapping_file)
#######################################################################


def test_no_duplicates():
    """
    Relations/ways are only inserted once
    Checks #66
    """
    highways = t.query_duplicates(t.db_conf, 'osm_roads')
    # one duplicate for test_node_way_inserted_twice is expected
    assert highways == [[18001, 2]], highways
    landusages = t.query_duplicates(t.db_conf, 'osm_landusages')
    assert not landusages, landusages

def test_updated_landusage():
    """Multipolygon relation was modified"""
    t.assert_cached_node(1001, (13.5, 47.5))
    landusage_1001 = t.query_row(t.db_conf, 'osm_landusages', -1001)
    # point not in polygon after update
    assert not landusage_1001['geometry'].intersects(t.merc_point(13.4, 47.5))

def test_partial_delete():
    """Deleted relation but nodes are still cached"""
    t.assert_cached_node(2001)
    t.assert_cached_way(2001)
    t.assert_cached_way(2002)
    assert not t.query_row(t.db_conf, 'osm_landusages', -2001)
    assert not t.query_row(t.db_conf, 'osm_landusages', 2001)

def test_updated_nodes():
    """Nodes were added, modified or deleted"""
    t.assert_missing_node(10000)
    t.assert_cached_node(10001, (10.0, 40.0))
    t.assert_cached_node(10002, (10.1, 40.0))
    place_10001 = t.query_row(t.db_conf, 'osm_places', 10001)
    assert place_10001['name'] == 'Bar', place_10001
    place_10002 = t.query_row(t.db_conf, 'osm_places', 10002)
    assert place_10002['name'] == 'Baz', place_10002

def test_landusage_to_waterarea_2():
    """Parks converted to water moved from landusages to waterareas"""
    t.assert_cached_way(11001)
    t.assert_cached_way(12001)
    t.assert_cached_way(13001)

    assert not t.query_row(t.db_conf, 'osm_landusages', 11001)
    assert not t.query_row(t.db_conf, 'osm_landusages', -12001)
    assert not t.query_row(t.db_conf, 'osm_landusages', -13001)

    assert not t.query_row(t.db_conf, 'osm_landusages_gen0', 11001)
    assert not t.query_row(t.db_conf, 'osm_landusages_gen0', -12001)
    assert not t.query_row(t.db_conf, 'osm_landusages_gen0', -13001)

    assert not t.query_row(t.db_conf, 'osm_landusages_gen1', 11001)
    assert not t.query_row(t.db_conf, 'osm_landusages_gen1', -12001)
    assert not t.query_row(t.db_conf, 'osm_landusages_gen1', -13001)

    assert t.query_row(t.db_conf, 'osm_waterareas', 11001)['type'] == 'water'
    assert t.query_row(t.db_conf, 'osm_waterareas', -12001)['type'] == 'water'
    assert t.query_row(t.db_conf, 'osm_waterareas', -13001)['type'] == 'water'

    assert t.query_row(t.db_conf, 'osm_waterareas_gen0', 11001)['type'] == 'water'
    assert t.query_row(t.db_conf, 'osm_waterareas_gen0', -12001)['type'] == 'water'
    assert t.query_row(t.db_conf, 'osm_waterareas_gen0', -13001)['type'] == 'water'

    assert t.query_row(t.db_conf, 'osm_waterareas_gen1', 11001)['type'] == 'water'
    assert t.query_row(t.db_conf, 'osm_waterareas_gen1', -12001)['type'] == 'water'
    assert t.query_row(t.db_conf, 'osm_waterareas_gen1', -13001)['type'] == 'water'

def test_changed_hole_tags_2():
    """Newly tagged hole is inserted"""
    t.assert_cached_way(14001)
    t.assert_cached_way(14011)

    assert t.query_row(t.db_conf, 'osm_waterareas', 14011)['type'] == 'water'
    assert t.query_row(t.db_conf, 'osm_landusages', -14001)['type'] == 'park'
    t.assert_almost_equal(t.query_row(t.db_conf, 'osm_waterareas', 14011)['geometry'].area, 26672000000, -6)
    t.assert_almost_equal(t.query_row(t.db_conf, 'osm_landusages', -14001)['geometry'].area, 10373600000, -6)

def test_split_outer_multipolygon_way_2():
    """Splitted outer way of multipolygon was inserted"""
    data = t.cache_query(ways=[15001, 15002], deps=True)
    assert data['ways']['15001']['relations'].keys() == ['15001']
    assert data['ways']['15002']['relations'].keys() == ['15001']

    assert t.query_row(t.db_conf, 'osm_landusages', 15001) == None
    park_15001 = t.query_row(t.db_conf, 'osm_landusages', -15001)
    assert park_15001['type'] == 'park'
    t.assert_almost_equal(park_15001['geometry'].area, 9816216452, -1)
    assert t.query_row(t.db_conf, 'osm_roads', 15002)['type'] == 'residential'

def test_merge_outer_multipolygon_way_2():
    """Merged outer way of multipolygon was inserted"""
    data = t.cache_query(ways=[16001, 16002], deps=True)
    assert data['ways']['16001']['relations'].keys() == ['16001']
    assert data['ways']['16002'] == None

    data = t.cache_query(relations=[16001], full=True)
    assert sorted(data['relations']['16001']['ways'].keys()) == ['16001', '16011']

    assert t.query_row(t.db_conf, 'osm_landusages', 16001) == None
    park_16001 = t.query_row(t.db_conf, 'osm_landusages', -16001)
    assert park_16001['type'] == 'park'
    t.assert_almost_equal(park_16001['geometry'].area, 12779350582, -1)
    assert t.query_row(t.db_conf, 'osm_roads', 16002) == None

def test_node_way_ref_after_delete_2():
    """Node does not referece deleted way"""
    data = t.cache_query(nodes=[20001, 20002], deps=True)
    assert 'ways' not in data['nodes']['20001']
    assert data['nodes']['20002'] == None
    assert t.query_row(t.db_conf, 'osm_roads', 20001) == None
    assert t.query_row(t.db_conf, 'osm_barrierpoints', 20001)['type'] == 'block'

def test_way_rel_ref_after_delete_2():
    """Way does not referece deleted relation"""
    data = t.cache_query(ways=[21001], deps=True)
    assert 'relations' not in data['ways']['21001']
    assert t.query_row(t.db_conf, 'osm_roads', 21001)['type'] == 'residential'
    assert t.query_row(t.db_conf, 'osm_landusages', 21001) == None
    assert t.query_row(t.db_conf, 'osm_landusages', -21001) == None

def test_residential_to_secondary2():
    """New secondary (from residential) is now in roads_gen0/1."""
    assert t.query_row(t.db_conf, 'osm_roads', 40001)['type'] == 'secondary'
    assert t.query_row(t.db_conf, 'osm_roads_gen0', 40001)['type'] == 'secondary'
    assert t.query_row(t.db_conf, 'osm_roads_gen1', 40001)['type'] == 'secondary'

def test_relation_after_remove():
    """Relation is deleted and way is still present."""
    assert t.query_row(t.db_conf, 'osm_buildings', 50011)['type'] == 'yes'
    assert t.query_row(t.db_conf, 'osm_landusages', 50021) == None
    assert t.query_row(t.db_conf, 'osm_landusages', -50021) == None

def test_relation_without_tags2():
    """Relation without tags is removed."""
    t.cache_query(ways=[50111], deps=True)
    assert t.cache_query(relations=[50121], deps=True)['relations']["50121"] == None

    assert t.query_row(t.db_conf, 'osm_buildings', 50111)['type'] == 'yes'
    assert t.query_row(t.db_conf, 'osm_buildings', 50121) == None
    assert t.query_row(t.db_conf, 'osm_buildings', -50121) == None

def test_duplicate_ids2():
    """Only relation/way with same ID was deleted."""
    assert t.query_row(t.db_conf, 'osm_buildings', 51001)['type'] == 'way'
    assert t.query_row(t.db_conf, 'osm_buildings', -51001) == None
    assert t.query_row(t.db_conf, 'osm_buildings', -51011)['type'] == 'mp'
    assert t.query_row(t.db_conf, 'osm_buildings', 51011) == None

def test_updated_way2():
    """All nodes of straightened way are updated."""
    road =  t.query_row(t.db_conf, 'osm_roads', 60000)
    # new length 0.1 degree
    t.assert_almost_equal(road['geometry'].length, 20037508.342789244/180.0/10.0)

def test_update_node_to_coord_2():
    """Node is becomes coord after tags are removed."""
    coords = t.cache_query(nodes=(70001, 70002))

    assert "tags" not in coords['nodes']["70001"]
    assert coords['nodes']["70002"]["tags"] == {"amenity": "police"}

    assert not t.query_row(t.db_conf, 'osm_amenities', 70001)
    assert t.query_row(t.db_conf, 'osm_amenities', 70002)

def test_no_duplicate_insert():
    """
    Relation is not inserted again if a nother relation with the same way was modified
    Checks #65
    """
    assert t.query_row(t.db_conf, 'osm_landusages', -201191)['type'] == 'park'
    assert t.query_row(t.db_conf, 'osm_landusages', -201192)['type'] == 'forest'
    assert t.query_row(t.db_conf, 'osm_roads', 201151)['type'] == 'residential'

def test_unsupported_relation():
    """
    Unsupported relation type is not inserted with update
    """
    assert not t.query_row(t.db_conf, 'osm_landusages', -201291)
    assert t.query_row(t.db_conf, 'osm_landusages', 201251)['type'] == 'park'

#######################################################################
def test_deploy_and_revert_deploy():
    """Revert deploy succeeds"""
    assert not t.table_exists('osm_roads', schema=t.TEST_SCHEMA_IMPORT)
    assert t.table_exists('osm_roads', schema=t.TEST_SCHEMA_PRODUCTION)
    assert not t.table_exists('osm_roads', schema=t.TEST_SCHEMA_BACKUP)

    # import again to have a new import schema
    t.imposm3_import(t.db_conf, './build/complete_db.pbf', mapping_file)
    assert t.table_exists('osm_roads', schema=t.TEST_SCHEMA_IMPORT)

    t.imposm3_deploy(t.db_conf, mapping_file)
    assert not t.table_exists('osm_roads', schema=t.TEST_SCHEMA_IMPORT)
    assert t.table_exists('osm_roads', schema=t.TEST_SCHEMA_PRODUCTION)
    assert t.table_exists('osm_roads', schema=t.TEST_SCHEMA_BACKUP)

    t.imposm3_revert_deploy(t.db_conf, mapping_file)
    assert t.table_exists('osm_roads', schema=t.TEST_SCHEMA_IMPORT)
    assert t.table_exists('osm_roads', schema=t.TEST_SCHEMA_PRODUCTION)
    assert not t.table_exists('osm_roads', schema=t.TEST_SCHEMA_BACKUP)

def test_remove_backup():
    """Remove backup succeeds"""
    assert t.table_exists('osm_roads', schema=t.TEST_SCHEMA_IMPORT)
    assert t.table_exists('osm_roads', schema=t.TEST_SCHEMA_PRODUCTION)
    assert not t.table_exists('osm_roads', schema=t.TEST_SCHEMA_BACKUP)

    t.imposm3_deploy(t.db_conf, mapping_file)

    assert not t.table_exists('osm_roads', schema=t.TEST_SCHEMA_IMPORT)
    assert t.table_exists('osm_roads', schema=t.TEST_SCHEMA_PRODUCTION)
    assert t.table_exists('osm_roads', schema=t.TEST_SCHEMA_BACKUP)

    t.imposm3_remove_backups(t.db_conf, mapping_file)

    assert not t.table_exists('osm_roads', schema=t.TEST_SCHEMA_IMPORT)
    assert t.table_exists('osm_roads', schema=t.TEST_SCHEMA_PRODUCTION)
    assert not t.table_exists('osm_roads', schema=t.TEST_SCHEMA_BACKUP)

