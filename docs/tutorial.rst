Tutorial
========

The ``imposm`` command has multiple sub-commands. This tutorial will explain the most important sub-commands: ``import`` and ``diff``.

Use the name of the sub-command as the second argument to start it. For example, to print the installed version number call the ``version`` sub-command::

  $ imposm version
  master-20180507-7ddba33


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
    echo "ALTER USER osm WITH PASSWORD 'osm';" |psql -d osm

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

  imposm import -mapping mapping.yml -read germany.osm.pbf


Cache files
~~~~~~~~~~~

Imposm stores the cache files in `/tmp/imposm`. You can change that path with ``-cachedir``. Imposm can merge multiple OSM files into the same cache (e.g. when combining multiple extracts) with the ``-appendcache`` option or it can overwrite existing caches with ``-overwritecache``. Imposm will fail to ``-read`` if it finds existing cache files and if you don't specify either ``-appendcache`` or ``-overwritecache``.

Make sure that you have enough disk space for storing these cache files. The underlying LevelDB library will crash if it runs out of free space. 2-3 times the size of the PBF file is a good estimate for the cache size, even with -diff mode.

Writing
-------

The second step is the writing of OpenStreetMap features into the database. It reads the features from the cache from step one, builds all geometries and imports them into the according tables. It overwrites existing tables, :ref:`see below <production_tables>` for more information on how to update your database in production.

After the import, it creates the generalized tables and indicies.

You need to tell Imposm the connection parameters of your database. The ``-connection`` option takes a URL in the format ``postgis://username:password@host:port/databasename`` or a list of parameters like ``postgis: host=/tmp dbname=osm``.

In our example:
::

  imposm import -mapping mapping.yml -write -connection postgis://osm:osm@localhost/osm

You can combine reading and writing::

  imposm import -mapping mapping.yml -read hamburg.osm.pbf -write -connection postgis://osm:osm@localhost/osm


All tables are prefixed with ``osm_``, e.g. ``roads`` will create the table ``osm_roads``. You can change the prefix by appending ``?prefix=myprefix`` to the connection URL. Use ``NONE`` to disable prefixing::

  imposm import -mapping mapping.yml -write -connection postgis://osm:osm@localhost/osm?prefix=NONE


Limit to
~~~~~~~~

You can limit the imported geometries to polygon boundaries. You can load the limit-to polygons from GeoJSON files. Line strings and polygons will be clipped exactly at the limit to geometry. The GeoJSON needs to be in EPSG:4326.

::

    imposm import -mapping mapping.yml -connection postgis://osm:osm@localhost/osm -read europe.osm.pbf -write -limitto germany.geojson


``-limitto`` also controls which elements are stored in the internal cache. You can configure a buffer around the ``-limitto`` geometry with the ``-limittocachebuffer`` to add more elements to your cache. This is necessary for getting complete polygons and line strings at the boundaries of your ``-limitto`` geometry.

Config file
~~~~~~~~~~~

You can create a simple JSON configuration file, instead of specifying the ``-connection`` or ``-mapping`` option with each run. You can use this configuration with the ``-config`` option.

You can configure the following options:

- ``cachedir``
- ``connection``
- ``limitto``
- ``limittocachebuffer``
- ``mapping``
- ``srid``
- ``diffdir``


Here is an example configuration::

    {
        "cachedir": "/tmp/imposm_cache",
        "connection": "postgis://osm:osm@localhost/osm",
        "mapping": "mapping.yml"
    }

And here is it in use::

    imposm import -config config.json -read hamburg.osm.pbf -write



Optimize
--------

This step is optional and it does some optimization on the created tables. It clusters each table based on the spatial index and does a vacuum analyze on the database. The optimizations only work with the import tables, but not the production tables (:ref:`see below <production_tables>`).

::

  imposm import -config config.json -optimize

You can combine reading, writing and optimizing::

  imposm import -config config.json -read hamburg.osm.pbf -write -optimize


.. _production_tables:

Deploy production tables
------------------------

Since Imposm overwrites existing tables on import (``-write``), it is recommended to use different schemas for import and for production.
Imposm imports all tables into the ``import`` schema by default. For example, after the import the table ``osm_roads`` is accessible as ``import.osm_roads`` and not as ``osm_roads`` or ``public.osm_roads``.

.. note:: Database schemas are a feature of a few databases including PostgreSQL to define multiple namespaces for tables. Don't mistake this for database schemas (as in data model) which are discussed in :doc:`mapping`.

Imposm can `deploy` all imported tables by updating the schema of the tables.
To move all tables form ``import`` to the default schema ``public``::

  imposm import -mapping mapping.yml -connection postgis://osm:osm@localhost/osm -deployproduction

This will also remove all existing Imposm tables from ``backup`` and it will moves tables from the ``public`` to the ``backup`` schema.

You can revert a deploy (moving ``public`` tables to ``import`` and ``backup`` tables to ``public``)::

  imposm import -mapping mapping.yml -connection postgis://osm:osm@localhost/osm -revertdeploy

And you can remove the backup schema::

  imposm import -mapping mapping.yml -connection postgis://osm:osm@localhost/osm -removebackup

You can change the schema names with ``dbschema-import``, ``-dbschema-production`` and ``-dbschema-backup``

Other options
-------------

Projection
~~~~~~~~~~

Imposm uses the the web mercator projection (``EPSG:3857``) for the imports. You can change this with the ``-srid`` option. At the moment only EPSG:3857 and EPSG:4326 are supported.

.. _diff:

Updating
^^^^^^^^

Imposm can keep the OSM data up-to-date by importing changes from `OSM changes files <http://wiki.openstreetmap.org/wiki/OsmChange>`_.
It needs to cache a few more information to be able to update the database from diff files. You can enable this with the `-diff` option during the initial import.

::

  imposm import -config config.json -read hamburg.osm.pbf -write -diff -cachedir ./cache -diffdir ./diff

.. note:: Each diff import requires access to the cache files from this initial import. So it is a good idea to set ``-cachedir`` to a permanent location instead of `/tmp/`.

.. note:: You should not make changes to the mapping file after the initial import. Changes are not detected and this can result aborted updates or incomplete data.

`run`
-----

Imposm can automatically fetch and import diff files. It stores the current sequence in `last.state.txt` inside the `-diffdir` directory. The downloaded diff files are cached in this directory as well.

To start the update process::

  imposm run -config config.json

You can stop processing new diff files SIGTERM (``crtl-c``), SIGKILL or SIGHUP. You should create systemd/upstart/init.d service for ``imposm run`` to always run in background.

You can change to hourly updates by adding `replication_url: "https://planet.openstreetmap.org/replication/hour/"` and `replication_interval: "1h"` to the Imposm configuration. Same for daily updates (works also for Geofabrik updates): `replication_url: "https://planet.openstreetmap.org/replication/day/"` and `replication_interval: "24h"`.

At import time, Imposm compute the first diff sequence number by comparing the PBF input file timestamp and the latest state available in the remote server. Depending on the PBF generation process, this sequence number may not be correct, you can force Imposm to start with an earlier sequence number by adding a `diff_state_before` duration in your conf file. For example, `diff_state_before: 4h` will start with an initial sequence number generated 4 hours before the PBF generation time.


One-time update
---------------

You can also manually update an existing database by importing `OSM changes files <http://wiki.openstreetmap.org/wiki/OsmChange>`_. Changes files contain all edits made to the OSM dataset in a defined time-range. These files are `available at planet.openstreetmap.org <http://wiki.openstreetmap.org/wiki/Planet.osm/diffs>`_.

The ``diff`` sub-command requires similar options as the ``import`` sub-command. You can pass one or more XML changes files to ``diff``, instead of a PBF file for the ``-read`` option.

To update an existing database with three change files::

  imposm diff -config config.json changes-1.osc.gz changes-2.osc.gz changes-3.osc.gz

Imposm stores the sequence number of the last imported changeset in `${cachedir}/last.state.txt`, if it finds a matching state file (`123.state.txt` for `123.osc.gz`). Imposm refuses to import the same diff files a second time if these state files are present.

Remember that you have to make the initial import with the ``-diff`` option. See above.

.. note:: You should not make changes to the mapping file after the initial import. Changes are not detected and this can result aborted updates or incomplete data.

Expire tiles
------------

Imposm can log where the OSM data was changed when it imports diff files. You can use the ``-expiretiles-dir`` option to specify a location where Imposm should log this information. Imposm creates files in the format `YYYYmmdd/HHMMSS.sss.tiles`` (e.g. ``20240629/212345.123.tiles``) inside this directory. The timestamp is the current time of the diff import, not the creation time of the diff. Each file contains a list with webmercator tiles in the format ``z/x/y`` (e.g. ``14/7321/1339``). All tiles are based on zoom level 14. You can change this with the ``-expiretiles-zoom`` option.
Imposm tries to keep the number of change tiles reasonable for large changes by "zooming out", e.g. a continent wide change would result in a few handful of tiles in zoom level 6, and not millions of tiles in level 14.
Both expire options can be set as ``expiretiles_dir`` and ``expiretiles_zoom`` in the JSON configuration.
