Data Mapping
============

The data mapping defines which `OSM feature types <http://wiki.openstreetmap.org/wiki/Map_Features>`_ should be imported in which table. The mapping is a YAML (or JSON) file.

See `example-mapping.yml <https://raw.githubusercontent.com/omniscale/imposm3/master/example-mapping.yml>`_ for an example.


Tables
------

The most important part is the ``tables`` definition. Each table is a YAML object with the table name as the key. Each table has a ``type``, a mapping definition and ``columns``.


``type``
~~~~~~~~

``type`` can be ``point``, ``linestring``, ``polygon``, ``geometry``, ``relation`` and ``relation_member``. ``geometry`` requires a special ``mapping``. :doc:`Relations are described in more detail here <relations>`.


``mapping``
~~~~~~~~~~~

``mapping`` defines which OSM key/values an element needs to have to be imported into this table. ``mapping`` is a YAML object with the OSM `key` as the object key and a list of all OSM `values` to be matched as the object value.
You can use ``__any__`` to match all values (e.g. ``amenity: [__any__]``). To match elements regardless of their tags use ``__any__: [__any__]``. You need to use :ref:`load_all<tags>` in this case so that Imposm has access to all tags.

To import all polygons with `tourism=zoo`, `natural=wood` or `natural=land` into the ``landusages`` table:

.. code-block:: yaml
   :emphasize-lines: 4-6

    tables:
      landusages:
        type: polygon
        mapping:
          natural: [wood, land]
          tourism: [zoo]
        …


``columns``
~~~~~~~~~~~

``columns`` is a list of columns that Imposm should create for this table. Each column is a YAML object with a ``type`` and a ``name`` and optionaly ``key``, ``args`` and ``from_member``.

``name``
^^^^^^^^^

This is the name of the resulting column.

``type``
^^^^^^^^

This defines the data type that Imposm should use for this column. There are two different classes of types. `Value types` are types that convert OSM tag values to a specific database type. Examples are ``string`` for street or place names, or ``bool`` for ``yes``, ``no``, ``true`` or ``false`` values.
`Element types` are types that dependent on the OSM element (node/way/relation). Examples are ``geometry`` for the geometry, ``mapping_key`` and ``mapping_value`` for the actual key and value that was mapped.

See :ref:`column_types` for documentation of all types.


``key``
^^^^^^^

``key`` defines the OSM `key` that should be used for this column. This is required for all `value types`.

``args``
^^^^^^^^

Some column types require additional arguments. Refer to the documentation of the type.

``from_member``
^^^^^^^^^^^^^^^

``from_member`` is only valid for tables of the type ``relation_member``. If this is set to ``true``, then tags will be used from the member instead of the relation.


Example
~~~~~~~

The mapping below will create a ``tracks`` table with the following columns:

- ``osm_id`` with the ID of the way
- ``the_geom`` with a `LineString` geometry
- ``street_name`` with the content of the OSM `name` tag
- ``is_bridge`` with a ``true`` value if the OSM `bridge` tag is `true`-ish (``1``, ``yes`` or ``true``), otherwise it will be ``false``
- ``highway_type`` with the OSM `value` that was matched by the ``mapping`` of this table. In this example one of ``path``, ``track``, or ``classified``.



.. code-block:: yaml

    tables:
      tracks:
        type: linestring
        mapping:
          highway: [path, track, unclassified]
        columns:
        - {name: osm_id, type: id}
        - {name: the_geom, type: geometry}
        - {key: name, name: street_name, type: string}
        - {key: bridge, name: is_bridge, type: bool}
        - {name: highway_type, type: mapping_value}



``mappings``
~~~~~~~~~~~~

An OSM element is only inserted once even if a mapping matches multiple tags. Sometime it's convenient to have a geometry multiple times, e.g. a way with ``rail=tram`` and ``highway=secondary``.
``mappings`` allows to define multiple sub-mappings. Each sub-mapping requires a name and a separate mapping dictionary. The elements will be inserted into the table for each match of a sub-mapping.


.. code-block:: yaml
   :emphasize-lines: 4-10

    tables:
      transport:
        type: linestring
        mappings:
          rail:
            mapping:
              rail: [__any__]
          roads:
            mapping:
              highway: [__any__]
          …


.. _column_types:


Column types
------------

Value types
~~~~~~~~~~~

``bool``
^^^^^^^^

Convert ``true``, ``yes`` and ``1`` values to ``true``, otherwise use ``false``.

``boolint``
^^^^^^^^^^^

Same as ``bool`` but stores a numeric ``1`` for ``true`` values, and ``0`` otherwise.


``string``
^^^^^^^^^^

The value as-is. Note that missing values will be inserted as an empty string and not as ``null``. This allows SQL queries like ``column NOT IN ('a', 'b')``.


``direction``
^^^^^^^^^^^^^

Convert ``true``, ``yes`` and ``1`` to the numeric ``1``, ``-1`` values to ``-1`` and other values to ``0``. This is useful for oneways where a -1 signals that a oneway goes in the opposite direction of the geometry.


``integer``
^^^^^^^^^^^

Convert values to an integer number. Other values will not be inserted. Useful for ``admin_levels`` for example.


``enumerate``
^^^^^^^^^^^^^

Enumerates a list of values and stores tag values as an integer.

The following `enum` column will contain ``1`` for ``landuse=forest``, ``4`` for ``landuse=grass`` and ``0`` for undefined values.

.. code-block:: yaml

  columns:
    - name: enum
      type: enumerate
      key: landuse
      args:
          values:
             - forest
             - park
             - cemetery
             - grass


``mapping_value`` will be used when ``key`` is not set or ``null``.


Element types
~~~~~~~~~~~~~


``id``
^^^^^^

The ID of the OSM node, way or relation. Relation IDs are negated (-1234 for ID 1234) to prevent collisions with way IDs.


``mapping_key``
^^^^^^^^^^^^^^^

The OSM `key` that was matched by this table mapping (`highway`, `building`, `nature`, `landuse`, etc.).

.. note:: Imposm will choose the first key of the table mapping if an OSM element has multiple tags that match.
  For example: `mapping_key` will use `natural` for an OSM element with `landuse=forest` and `natural=wood` tags, if `natural` comes before `landuse` in the table mapping. You need to define an explicit column if you need the value of a specific tag (e.g. `{"type": "string", "name": "landuse", "key": "landuse"}`).

``mapping_value``
^^^^^^^^^^^^^^^^^

The OSM `value` that was matched by this table mapping (`primary`, `secondary`, `yes`, `forest`, etc.).

..note:: The note of ``mapping_key`` above applies to ``mapping_values`` as well.

``geometry``
^^^^^^^^^^^^

The geometry of the OSM element.


``validated_geometry``
^^^^^^^^^^^^^^^^^^^^^^

Like `geometry`, but the geometries will be validated and repaired when this table is used as a source for a generalized table. Must only be used for `polygon` tables.


``pseudoarea``
^^^^^^^^^^^^^^

Area of polygon geometries in square meters. This area is calculated in the webmercator projection, so it is only accurate at the equator and gets off the more the geometry moves to the poles. It's still good enough to sort features by area for rendering purposes.


``wayzorder``
^^^^^^^^^^^^^

Calculate the z-order of an OSM highway or railway. Returns a numeric value that represents the importance of a way where ``motorway`` is the most important (9), and ``path`` or ``track`` are least important (0). ``bridge`` and ``tunnel``  will modify the value by -10/+10. ``layer`` will be multiplied by ten and added to the value. E.g. ``highway=motorway``, ``bridge=yes`` and ``layer=2`` will return 39 (9+10+2*10).


``hstore_tags``
^^^^^^^^^^^^^^^

Stores all tags in a HStore column. Requires the PostGIS HStore extension. This will only insert tags that are referenced in the ``mapping`` or ``columns`` of any table. See :ref:`tags` on how to import all availabel tags.


.. TODO
.. "string_suffixreplace": {"string_suffixreplace", "string", nil, MakeSuffixReplace},


Element types for ``relation_member``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following types are only valid for tables of the type ``relation_member``.

``member_id``
^^^^^^^^^^^^^

The OSM ID of the relation member.

``member_type``
^^^^^^^^^^^^^^^

The type of the relation member. 0 for nodes, 1 for ways and 2 for relations.


``member_role``
^^^^^^^^^^^^^^^

The role of the relation member as a string, e.g. `outer`, `stop`, etc.


``member_index``
^^^^^^^^^^^^^^^^

The index of the member in the relation, starting from 0. E.g. the first member is 0, second member is 1, etc.
This can be used to query bus stops of a route relation in the right order.


Generalized Tables
------------------


Generalized tables allow you to create a copy of an imported table with simplified/generalized geometries. You can use these generalized tables for rendering low map scales, where a high spatial resolution is not required.

Each generalize table is a YAML object with the new table name as the key. Each generalize table has a ``source`` and a ``tolerance`` and optionally an ``sql_filter``.

``source`` is the table name of another Imposm table from the same mapping file. You can also reference another generalized table, to create multiple generalizations of the same data.

``tolerance`` is the `resolution` used for the Douglas-Peucker simplification. It has the same unit as the import `-srid`, i.e. meters for ESPG:3857 (equivalent to OpenLayers:900913) and degrees for EPSG:4326. Imposm uses `PostGIS ST_SimplifyPreserveTopology <http://postgis.net/docs/ST_SimplifyPreserveTopology.html>`_.

The optional ``sql_filter`` can be used to limit the rows that will be generalized. You can use it to drop geometries that are to small for the target map scale.

.. code-block:: yaml

    generalized_tables:
      waterareas_gen_50:
        source: waterareas
        sql_filter: ST_Area(geometry)>50000.000000
        tolerance: 50.0



.. _tags:

Tags
----

Imposm caches only tags that are required for a ``mapping`` or for any ``columns``. This keeps the cache small as it does not store any tags that are not required for the import. You can change this if you want to import all tags, e.g with the ``hstore_tags`` column type.

Add ``load_all`` to the ``tags`` object inside your mapping YAML file. You can still exclude tags with the ``exclude`` option. ``exclude`` supports a simple shell file name pattern matching.

To load all tags except ``created_by``, ``source``, and ``tiger:county``, ``tiger:tlid``, ``tiger:upload_uuid``, etc:

.. code-block:: yaml

    tags:
      load_all: true,
      exclude: [created_by, source, "tiger:*"]
