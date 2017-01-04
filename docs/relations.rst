Relations
=========

In `OpenStreetMap, relations <http://wiki.openstreetmap.org/wiki/Relation>`_ define logical or geographic relationships between other nodes, ways and relations.

The most common relation type is a multipolygon, but all other relations can be imported as well.

Multipolygons
-------------

`Multipolygon relations <http://wiki.openstreetmap.org/wiki/Relation:multipolygon>`_ are used to represent complex polygon geometries. They are also the only way to represent holes in polygons.


Multipolygon relations are automatically handled by Imposm for all ``polygon`` tables.

The following mapping::

    tables:
      buildings:
        type: polygon
        mapping:
          building: [__any__]


Inserts closed ways if they have a ``building`` tag::

  <way id="1001" version="1" timestamp="2011-11-11T00:11:11Z">
    <nd ref="1001"/>
    ...
    <nd ref="1001"/>
    <tag k="building" v="yes"/>
  </way>

It will also insert relations of the type ``multipolygon`` with a ``building`` tag::

  <relation id="17101" version="1" timestamp="2011-11-11T00:11:11Z">
    <member type="way" ref="17101" role="outer"/>
    <member type="way" ref="17102" role="outer"/>
    <tag k="type" v="multipolygon"/>
    <tag k="building" v="yes"/>
  </relation>

The roles are ignored by Imposm as not all holes are correctly tagged as ``inner``. Imposm uses geometry operations to verify if a member of a multipolygon is a hole, or if it is a separate polygon.


For compatibility, multipolygon relations without tags will use the tags from the (longest) outer way. Imposm will insert the following relation as well::

  <way id="18101" version="1" timestamp="2011-11-11T00:11:11Z">
    <nd ref="1001"/>
    ...
    <nd ref="1001"/>
    <tag k="building" v="yes"/>
  </way>
  <relation id="18901" version="1" timestamp="2011-11-11T00:11:11Z">
    <member type="way" ref="18101" role="outer"/>
    <member type="way" ref="18102" role="outer"/>
    <tag k="type" v="multipolygon"/>
  </relation>



Other relations
---------------

OpenStreetMap also uses relations to map more complex features. Some examples:

    - `Administrative areas <http://wiki.openstreetmap.org/wiki/Relation:boundary>`_ with boundaries, capitals and label positions.
    - `Bus/tram/train routes <http://wiki.openstreetmap.org/wiki/Relation:route>`_ with the route itself, stops and platforms.
    - `3D buildings <http://wiki.openstreetmap.org/wiki/Simple_3D_buildings>`_ with multiple parts that should not be computed as holes.

These relations can not be mapped to `simple` linestrings or polygons as they can contain a mix of different geometry types, or would result in invalid geometries (overlapping polygons).

The Imposm table types ``relation`` and ``relation_member`` allow you to import all relevant data for these relations.

.. note:: ``relation`` and ``relation_member`` require :ref:`load_all<tags>` to have access to all keys.

``relation_member``
^^^^^^^^^^^^^^^^^^^

The ``relation_member`` table type inserts each member of the relation as a separate row. The ``relation_member`` has access to the `role` and `type` value of each member.  You can also import tags from the relation `and` from the member node, way or relation.

Example
~~~~~~~

You can use the following mapping::

  route_members:
    type: relation_member
    columns:
    - name: osm_id
      type: id
    - name: member
      type: member_id
    - name: index
      type: member_index
    - name: role
      type: member_role
    - name: type
      type: member_type
    - name: geometry
      type: geometry
    - name: relname
      key: name
      type: string
    - name: name
      key: name
      type: string
      from_member: true
    - key: ref
      name: ref
      type: string
    mapping:
      route: [bus]


to import a bus relation with stops, a platform and the route itself::

 <relation id="100901" version="1" timestamp="2015-06-02T04:13:19Z">
  <member type="node" ref="100101" role="stop_entry_only"/>
  <member type="node" ref="100102" role="stop"/>
  <member type="way" ref="100511" role="platform"/>
  <member type="node" ref="100103" role="stop_exit_only"/>
  <member type="way" ref="100501" role=""/>
  <member type="way" ref="100502" role=""/>
  <member type="way" ref="100503" role=""/>
  <tag k="name" v="Bus 301: A =&gt; B"/>
  <tag k="network" v="ABC"/>
  <tag k="ref" v="301"/>
  <tag k="route" v="bus"/>
  <tag k="type" v="route"/>
 </relation>

This will result in seven rows with the following columns:

======== ======================================================================================================================================================
Column   Description
======== ======================================================================================================================================================
osm_id   The ID of the relation. 100901 for all members.
member   The ID of the member. 100101, 100102, etc.
index    The index of the member. From 1 for 100101 to 7 for 100503. This can be used to query the bus stops in the correct order.
role     The role of the member. ``stop``, ``platform``, etc.
type     0 for nodes, 1 for ways and 2 for other relations.
geometry The geometry of the member. Point for nodes and linestring for ways.
relname  The value of the ``name`` tag of the relation. ``Bus 301: A => B`` in this case.
name     The value of the ``name`` tag of the member element, if it has one. Note that the mapping contains ``from_member: true`` for this column.
ref      The value of the ``ref`` tag of the relation. ``301`` in this case.
======== ======================================================================================================================================================


You can insert the tags of the relation in a separate ``relation`` table to avoid duplication and then use `joins` when querying the data.
Both ``osm_id`` and ``member_id`` columns are indexed in PostgreSQL by default to speed up these joins.

``relation``
^^^^^^^^^^^^

The ``relation`` table type inserts the mapped element regardless of the resulting geometry. For example, this allows you to create a table with the metadata (name, reference, operator, etc.) of all available route relations. The actual geometries need to be `joined` form the members.

Example
~~~~~~~

The following mapping imports the bus route relation from above::

  routes:
    type: relation
    columns:
    - name: osm_id
      type: id
    - key: ref
      name: ref
      type: string
    - name: network
      key: network
      type: string
    mapping:
      route: [bus]


This will create a single row with the mapped columns.

.. note:: ``relation`` tables do not support geometry columns. Use the geometries of the members, or use a ``polygon`` table if your relations contain multipolygons.


