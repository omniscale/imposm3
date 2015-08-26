Tutorial
========

The ``imposm3`` command has multiple sub-commands. This tutorial will explain the most important sub-commands: ``import`` and ``diff``.

Use the name of the sub-command as the second argument to start it. For example, to print the installed version number call the ``version`` sub-command::

  $ imposm3 version
  0.1dev-20150507-7ddba33


Preparation
^^^^^^^^^^^

Create database
---------------

This is step zero, since you have to do it only once. The following commands create a new PostgreSQL user (`osm`) and database (`osm`) with the PostGIS and the `hstore` extension enabled.

::

    sudo su postgres
    createuser --no-superuser --no-createrole --createdb osm
    createdb -E UTF8 -O osm osm
    psql -d osm -c "CREATE EXTENSION postgis;"
    psql -d osm -c "CREATE EXTENSION hstore;" # only required for hstore support
    echo "ALTER USER osm WITH PASSWORD \'osm\';" |psql -d osm

You can change the names if you like, but we will use `osm` for user name, password and database name in all following examples.

You also need to make sure that the user is allowed to access the database from localhost. Your PostgreSQL `pg_hba.conf` should contain a line like::

    host    all             all             127.0.0.1/32          md5

(Don't forget to reload PostgreSQL after these changes.)

The following command should print the PostGIS version, if everything was successful::

    PGPASSWORD=osm psql -h 127.0.0.1 -d osm -U osm -c 'select postgis_version();'

Please `refer to the PostGIS <http://postgis.net/docs/index.html>`_ and `PostgreSQL documentation <http://www.postgresql.org/docs/9.3/interactive/manage-ag-createdb.html>`_ for more information.


Importing
^^^^^^^^^

The import process is separated into multiple steps.
You can combine most steps in one command, but we will explain each one in detail here.

Reading
-------

The first step is the reading of the OpenStreetMap data. Building the way and relation geometries requires random access to all nodes and ways, but this is not supported by the OSM PBF data format. Imposm needs to store all nodes, ways and relations in an intermediary data store that allows random access to these elements. It does this on-disk to keep the memory usage of Imposm low. Having lots of memory will still speed the import up, because your OS will use all free memory for caching of these files.
Imposm uses LevelDB key-value databases for this, which are fast and compact.

Imposm needs to know which OSM elements you want to have in your database. You can use the provided ``mapping.yml`` file for this tutorial, but you should read :doc:`mapping` for more information on how to define your own mapping.


To read an extract::

  imposm3 import -mapping mapping.yml -read germany.osm.pbf


Cache files
~~~~~~~~~~~

Imposm stores the cache files in `/tmp/imposm3`. You can change that path with ``-cachedir``. Imposm can merge multiple OSM files into the same cache (e.g. when combining multiple extracts) with the ``-appendcache`` option or it can overwrite existing caches with ``-overwritecache``. Imposm will fail to ``-read`` if it finds existing cache files and if you don't specify either ``-appendcache`` or ``-overwritecache``.


Writing
-------

The second step is the writing of OpenStreetMap features into the database. It reads the features from the cache from step one, builds all geometries and imports them into the according tables. It overwrites existing tables, :ref:`see below <production_tables>` for more information on how to update your database in production.

After the import, it creates the generalized tables and indicies.

You need to tell Imposm the connection parameters of your database. The ``-connection`` option takes a URL in the format ``postgis://username:password@host:port/databasename``.

In our example:
::

  imposm3 import -mapping mapping.yml -write -connection postgis://osm:osm@localhost/osm

You can combine reading and writing::

  imposm3 import -mapping mapping.yml -read hamburg.osm.pbf -write -connection postgis://osm:osm@localhost/osm


Limit to
~~~~~~~~

You can limit the imported geometries to polygon boundaries. You can load the limit-to polygons from GeoJSON files. Line strings and polygons will be clipped exactly at the limit to geometry. The GeoJSON needs to be in EPSG:4326.

::

    imposm3 import -mapping mapping.yml -connection postgis://osm:osm@localhost/osm -read europe.osm.pbf -write -limitto germany.geojson


``-limitto`` also controls which elements are stored in the internal cache. You can configure a buffer around the ``-limitto`` geometry with the ``-limittocachebuffer`` to add more elements to your cache. This is necessary for getting complete polygons and line strings at the boundaries of your ``-limitto`` geometry.

Config file
~~~~~~~~~~~

You can create a simple Imposm configuration, instead of specifying the ``-connection`` or ``-mapping`` option with each run. You can use this configuration with the ``-config`` option.

You can configure the following options:

- ``cachedir``
- ``connection``
- ``limitto``
- ``limittocachebuffer``
- ``mapping``
- ``srid``


Here is an example configuration::

    {
        "cachedir": "/tmp/imposm3_cache",
        "connection": "postgis://osm:osm@localhost/osm",
        "mapping": "mapping.yml"
    }

And here is it in use::

    imposm3 import -config config.json -read hamburg.osm.pbf -write



Optimize
--------

This step is optional and it does some optimization on the created tables. It clusters each table based on the spatial index and does a vacuum analyze on the database. The optimizations only work with the import tables, but not the production tables (:ref:`see below <production_tables>`).

::

  imposm3 import -config config.json -optimize

You can combine reading, writing and optimizing::

  imposm3 import -config config.json -read hamburg.osm.pbf -write -optimize


.. _production_tables:

Deploy production tables
------------------------

Since Imposm overwrites existing tables on import (``-write``), it is recommended to use different schemas for import and for production.
Imposm imports all tables into the ``import`` schema by default. For example, after the import the table ``osm_roads`` is accessible as ``import.osm_roads`` and not as ``osm_roads`` or ``public.osm_roads``.

.. note:: Database schemas are a feature of a few databases including PostgreSQL to define multiple namespaces for tables. Don't mistake this for database schemas (as in data model) which are discussed in doc:`mapping`.

Imposm can `deploy` all imported tables by updating the schema of the tables.
To move all tables form ``import`` to the default schema ``public``::

  imposm3 import -mapping mapping.yml -connection postgis://osm:osm@localhost/osm -deployproduction

This will also remove all existing Imposm tables from ``backup`` and it will moves tables from the ``public`` to the ``backup`` schema.

You can revert a deploy (moving ``public`` tables to ``import`` and ``backup`` tables to ``public``)::

  imposm3 import -mapping mapping.yml -connection postgis://osm:osm@localhost/osm -revertdeploy

And you can remove the backup schema::

  imposm3 import -mapping mapping.yml -connection postgis://osm:osm@localhost/osm -removebackup

You can change the schema names with ``dbschema-import``, ``-dbschema-production`` and ``-dbschema-backup``

Other options
-------------

Projection
~~~~~~~~~~

Imposm uses the the web mercator projection (``EPSG:3857``) for the imports. You can change this with the ``-srid`` option. At the moment only EPSG:3857 and EPSG:4326 are supported.


Diff
~~~~

Imposm needs to cache a few more information to be able to update the database from OSM diff files. You can enable this with the `-diff` option.

::

  imposm3 import -config config.json -read hamburg.osm.pbf -write -diff

Read :ref:`diff` for more information.

.. note:: Each diff import requires access to the cache files from this initial import. So it is a good idea to set ``-cachedir`` to a premanent location instead of `/tmp/`.


.. _diff:

Updating
^^^^^^^^

Imposm allows you to update an existing database by importing changes from an `OSM changes file <http://wiki.openstreetmap.org/wiki/OsmChange>`_. Changes files contain all edits made to the OSM dataset in a defined time-range. These files are `available at planet.openstreetmap.org <http://wiki.openstreetmap.org/wiki/Planet.osm/diffs>`_.

The ``diff`` sub-command requires similar options as the ``import`` sub-command. You can pass one or more XML changes files to ``diff``, instead of a PBF file for the ``-read`` option.

To update an existing database with three change files::

  imposm3 diff -config config.json changes-1.osc.gz changes-2.osc.gz changes-3.osc.gz

Remember that you have to make the initial import with the ``-diff`` option. See above.

.. note:: You should not make changes to the mapping file after the initial import. Changes are not detected and this can result aborted updates or incomplete data.
