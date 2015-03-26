Data Mapping
============

The data mapping defines which `OSM feature types <http://wiki.openstreetmap.org/wiki/Map_Features>`_ should be imported in which table. The mapping is described with a JSON file.

See `example-mapping.json <https://raw.githubusercontent.com/omniscale/imposm3/master/example-mapping.json>`_ for an example.


Tables
------

The most important part is the ``tables`` definition. Each table is a JSON object with the table name as the key. Each table has a ``type`` a mapping definition and ``columns``.


``type``
~~~~~~~~

``type`` can be ``point``, ``linestring``, ``polygon`` or ``geometry``. ``geometry`` requires a special ``mapping``.


``mapping``
~~~~~~~~~~~

``mapping`` defines which OSM key/values an element needs to have to be imported into this table. ``mapping`` is a JSON object with the OSM `key` as the object key and a list of all OSM `values` to be matched as the object value.

To import all polygons with `tourism=zoo`, `natural=wood` or `natural=land` into the ``landusages`` table:

.. code-block:: javascript
   :emphasize-lines: 4-11

    {
      "tables": {
        "landusages": {
          "type": "polygon",
          "mapping": {
            "tourism": [
               "zoo"
             ],
             "natural": [
               "wood",
               "land"
             ]
          },
          ...
        }
      }
    }




``columns``
~~~~~~~~~~~

``columns`` is a list of columns that Imposm should create for this table. Each column is a JSON object with a ``type`` and a ``name`` and optionaly ``key`` and ``args``.

``name``
^^^^^^^^^

This is the name of the resulting column.

``type``
^^^^^^^^

This defines the data type that Imposm should use for this column. There are two different classes of types. `Value types` are types that convert OSM values to database types. Examples are ``string`` for street or place names, ``bool`` for ``yes``, ``no``, ``true`` or ``false``.
`Element types` are types that dependent on the OSM lement (node/way/relation). Examples are ``geometry`` for the geometry, ``mapping_key`` and ``mapping_value`` for the actual key and value that was mapped.

See :ref:`column_types` for documentation of all types.


``key``
^^^^^^^

``key`` defines the OSM `key` that should be used for this column. This is required for all `value types`.

``args``
^^^^^^^^

Some column types require additional arguments. Refer to the documentation of the type.



Example
~~~~~~~

The mapping below will create a ``tracks`` table with the following columns:

- ``osm_id`` with the ID of the way
- ``the_geom`` with a `LineString` geometry
- ``street_name`` with the content of the OSM `name` tag
- ``is_bridge`` with a ``true`` value if the OSM `bridge` tag is `true`-ish (``1``, ``yes`` or ``true``), otherwise it will be ``false``
- ``highway_type`` with the OSM `value` that was matched by the ``mapping`` of this table. In this example one of ``path``, ``track``, or ``classified``.



.. code-block:: javascript

    {
      "tables": {
        "tracks": {
          "columns": [
            {
                "name": "osm_id",
                "type": "id"
            },
            {
                "name": "the_geom",
                "type": "geometry"
            },
            {
                "name": "street_name",
                "type": "string",
                "key": "name"
            },
            {
                "name": "is_bridge",
                "type": "bool",
                "key": "bridge"
            },
            {
                "name": "highway_type",
                "type": "mapping_value"
            }
          ],
          "type": "linestring",
          "mapping": {
            "highway": {
                "path",
                "track",
                "unclassified"
            }
          }
        }
      }
    }

.. _column_types:


Column types
------------

Value types
~~~~~~~~~~~

``bool``
^^^^^^^^

Convert ``true``, ``yes`` and ``1``` values to ``true``, otherwise use ``false``.

``boolint``
^^^^^^^^^^^

Same as ``bool`` but stores a numeric ``1`` for ``true`` values, and ``0`` otherwise.


``string``
^^^^^^^^^^

The value as-is. Missing values will be inserted as an empty string and not as ``null``.


``direction``
^^^^^^^^^^^^^

Convert ``true``, ``yes`` and ``1`` to the numeric ``1``, ``-1`` values to ``-1`` and other values to ``0``. This is useful for oneways where a -1 signals that a oneway goes in the opposite direction of the geometry.


``integer``
^^^^^^^^^^^

Convert values to an integer number. Other values will not be inserted. Useful for ``admin_levels`` for example.


Element types
~~~~~~~~~~~~~


``id``
^^^^^^

The ID of the OSM node, way or relation. Relation IDs are negated (-1234 for ID 1234) to avoid collisions with relation IDs.


``mapping_key``
^^^^^^^^^^^^^^^

The OSM `key` that was matched by this table mapping (`highway`, `building`, `nature`, `landuse`, etc.).

..note::
Imposm will choose a random key if an OSM element has multiple tags that match the table mapping.
For example: `mapping_key` will use either `landuse` or `natural` for an OSM element with `landuse=forest` and `natural=wood` tags, if both are included in the mapping. You need to define an explicit column if you need to know if a specific tag was matched (e.g. `{"type": "string", "name": "landuse", "key": "landuse"}`).

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

Area of polygon geometries in square meters. This area is calculated in the webmercator projection, so it is only accurate at the equator and gets off the more the geometry move to the poles. It's still good enough to sort features by area for rendering purposes.


``wayzorder``
^^^^^^^^^^^^^

Calculate the z-order of an OSM highway or railway. Returns a numeric value that represents the importance of a way where ``motorway`` is the most important (9), and ``path`` or ``track`` are least important (0). ``bridge`` and ``tunnel``  will modify the value by -10/+10. ``layer`` will be multiplied by ten and added to the value. E.g. ``highway=motorway``, ``bridge=yes`` and ``layer=2`` will return 39.



.. TODO
.. "hstore_tags":          {"hstore_tags", "hstore_string", HstoreString, nil},
.. "wayzorder":            {"wayzorder", "int32", WayZOrder, nil},
.. "zorder":               {"zorder", "int32", nil, MakeZOrder},
.. "string_suffixreplace": {"string_suffixreplace", "string", nil, MakeSuffixReplace},
