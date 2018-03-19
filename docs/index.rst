Imposm3
=======

Imposm3 is an importer for OpenStreetMap data. It reads PBF files and imports the data into PostgreSQL/PostGIS databases.
It is designed to create databases that are optimized for rendering/tile/map-services.

It is developed and supported by `Omniscale <http://omniscale.com>`_ and is released as open source under the `Apache Software License 2.0 <http://www.apache.org/licenses/LICENSE-2.0.html>`_. Imposm3 is a rewrite of Imposm 2 with even better performance and support for (minutely) diff updates.


Features
--------

Custom database schemas
  It creates separate tables for different feature types. This allows easier styling and better performance for rendering in tile or WMS services.

Multiple CPU/core support
  Imposm is parallel from the ground up. It distributes parsing and processing to multiple CPUs/cores.

Unify values
  For example, the boolean values `1`, `on`, `true` and `yes` all become ``TRUE``.

Filter by tags and values
  It only imports data you are going to render/use.

Efficient nodes cache
  It is necessary to store all nodes to build ways and relations. Imposm uses a file-based key-value database to cache this data. This reduces the memory usage.

Generalized tables
  It can automatically create tables with lower spatial resolutions, perfect for rendering large road networks in low resolutions for example.

Limit to polygons
  It can limit imported geometries to polygons from GeoJSON.

hstore support
  Don't know which tags you will be needing? Store all tags in an `hstore column <http://www.postgresql.org/docs/9.6/static/hstore.html>`_.


Support
-------

There is a `mailing list at Google Groups <http://groups.google.com/group/imposm>`_ for all questions. You can subscribe by sending an email to: imposm+subscribe@googlegroups.com

For commercial support `contact Omniscale <http://omniscale.com/contact>`_.

Development
-----------

The source code is available at: https://github.com/omniscale/imposm3/

You can report any issues at: https://github.com/omniscale/imposm3/issues

Contents
--------

.. toctree::
   :maxdepth: 2

   install
   tutorial
   mapping
   relations


.. Indices and tables
.. ==================
..
.. * :ref:`genindex`
.. * :ref:`modindex`
.. * :ref:`search`

