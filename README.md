Imposm
======

Imposm is an importer for OpenStreetMap data. It reads PBF files and imports the data into PostgreSQL/PostGIS. It can also automatically update the database with the latest changes from OSM.

It is designed to create databases that are optimized for rendering (i.e. generating tiles or for WMS services).

Imposm >=3 is written in Go and it is a complete rewrite of the previous Python implementation.
Configurations/mappings and cache files are not compatible with Imposm 2, but they share a similar architecture.

The development of Imposm is sponsored by [Omniscale](https://omniscale.com/).


Features
--------

* High-performance
* Diff support
* Custom database schemas
* Generalized geometries


### In detail


- High performance:
  Parallel from the ground up. It distributes parsing and processing to all available CPU cores.

- Custom database schemas:
  Creates tables for different data types. This allows easier styling and better performance for rendering in WMS or tile services.

- Unify values:
  For example, the boolean values `1`, `on`, `true` and `yes` all become ``TRUE``.

- Filter by tags and values:
  Only import data you are going to render/use.

- Efficient nodes cache:
  It is necessary to store all nodes to build ways and relations. Imposm uses a file-based key-value database to cache this data.

- Generalized tables:
  Automatically creates tables with lower spatial resolutions, perfect for rendering large road networks in low resolutions.

- Limit to polygons:
  Limit imported geometries to polygons from GeoJSON, for city/state/country imports.

- Easy deployment:
  Single binary with only runtime dependencies to common libs (GEOS and LevelDB).

- Automatic OSM updates:
  Includes background service (`imposm run`) that automatically downloads and imports the latest OSM changes.

- Route relations:
  Import all relation types including routes.

- Support for table namespace (PostgreSQL schema)


Performance
-----------

* Imposm makes full use of all available CPU cores
* Imposm uses bulk inserts into PostgreSQL with `COPY FROM`
* Imposm uses efficient intermediate caches for reduced IO load during ways and relations building


An import in diff-mode on a Hetzner PX121-SSD server (Intel Xeon E5-1650 v3 Hexa-Core, 256GB RAM and SSD RAID 1) of a 36GB planet PBF (2017-08-10) with generalized tables and spatial indices, etc. takes around 6:30h. This is for an import that is ready for minutely updates. The non-diff mode is even faster.

It's recommended that the memory size of the server is roughly twice the size of the PBF extract you are importing. For example: You should have 64GB RAM or more for a current (2017) 36GB planet file, 8GB for a 4GB regional extract, etc.
Imports without SSDs will take longer.

Current status
--------------

Imposm is used in production but there is no official 3.0 release yet.
Imposm >=3, successor of Imposm 2, was called "Imposm 3" and binaries were named `imposm3` during development. Since April 2018 the project is only called Imposm to allow semantic versioning beyond version 3.
The repository will be renamed to github.com/omniscale/imposm in the future.

### Planned features ###

There are a few features we like to see in Imposm:

* Support for other projections than EPSG:3857 or EPSG:4326
* Custom field/filter functions
* Official releases with binaries for more platforms

There is no roadmap however, as the implementation of these features largely depends on external funding.

Installation
------------

### Binary

[Binary releases are available at GitHub.](https://github.com/omniscale/imposm3/releases)

These builds are for x86 64bit Linux and require *no* further dependencies. Download, untar and start `imposm`.
Binaries are compatible with Debian 8, Ubuntu 14.04 and SLES 12 (and newer versions). Older Imposm binaries (<=0.4) also support Debian 6, RHEL 6 and SLES 11.
Older versions are available at <http://imposm.org/static/rel/>.

### Source

There are some dependencies:

#### Compiler

You need [Go >=1.10](http://golang.org).

#### C/C++ libraries

Other dependencies are [libleveldb][] and [libgeos][].
Imposm was tested with recent versions of these libraries, but you might succeed with older versions.
GEOS >=3.2 is recommended, since it became much more robust when handling invalid geometries.


[libleveldb]: https://github.com/google/leveldb/
[libgeos]: http://trac.osgeo.org/geos/

#### Compile

Create a [Go workspace](http://golang.org/doc/code.html) by creating the `GOPATH` directory for all your Go code, if you don't have one already:

    mkdir -p go
    cd go
    export GOPATH=`pwd`
If you have Go >=1.16, you need to run this command to not use module-aware mode:

    go env -w GO111MODULE=auto

Get the code and install Imposm:

    go get github.com/omniscale/imposm3
    go install github.com/omniscale/imposm3/cmd/imposm

Done. You should now have an imposm binary in `$GOPATH/bin`.

Go compiles to static binaries and so Imposm has no runtime dependencies to Go.
Just copy the `imposm` binary to your server for deployment. The C/C++ libraries listed above are still required though.

See also `packaging.sh` for instructions on how to build binary packages for Linux.

#### LevelDB

For better performance you can either use [HyperLevelDB][libhyperleveldb] as an in-place replacement for libleveldb or you can use LevelDB >1.21. You need to build Imposm with ``go build -tags="ldbpost121"`` or ``LEVELDB_POST_121=1 make build`` to enable optimizations available with LevelDB 1.21 and higher.

[libhyperleveldb]: https://github.com/rescrv/HyperLevelDB

Usage
-----

`imposm` has multiple subcommands. Use `imposm import` for basic imports.

For a simple import:

    imposm import -connection postgis://user:password@host/database \
        -mapping mapping.json -read /path/to/osm.pbf -write

You need a JSON file with the target database mapping. See `example-mapping.json` to get an idea what is possible with the mapping.

Imposm creates all new tables inside the `import` table schema. So you'll have `import.osm_roads` etc. You can change the tables to the `public` schema:

    imposm import -connection postgis://user:passwd@host/database \
        -mapping mapping.json -deployproduction


You can write some options into a JSON configuration file:

    {
        "cachedir": "/var/local/imposm",
        "mapping": "mapping.json",
        "connection": "postgis://user:password@localhost:port/database"
    }

To use that config:

    imposm import -config config.json [args...]

For more options see:

    imposm import -help


Note: TLS/SSL support is disabled by default due to the lack of renegotiation support in Go's TLS implementation. You can re-enable encryption by setting the `PGSSLMODE` environment variable or the `sslmode` connection option to `require` or `verify-full`, eg: `-connect postgis://host/dbname?sslmode=require`. You will need to disable renegotiation support on your server to prevent connection errors on larger imports. You can do this by setting `ssl_renegotiation_limit` to 0 in your PostgreSQL server configuration.


Documentation
-------------

The latest documentation can be found here: <http://imposm.org/docs/imposm3/latest/>

Support
-------

There is a [mailing list at Google Groups](http://groups.google.com/group/imposm) for all questions. You can subscribe by sending an email to: `imposm+subscribe@googlegroups.com`

For commercial support [contact Omniscale](http://omniscale.com/contact).

Development
-----------

The source code is available at: <https://github.com/omniscale/imposm3/>

You can report any issues at: <https://github.com/omniscale/imposm3/issues>

License
-------

Imposm is released as open source under the Apache License 2.0. See LICENSE.

All dependencies included as source code are released under a BSD-ish license. See LICENSE.dep.

All dependencies included in binary releases are released under a BSD-ish license except the GEOS package.
The GEOS package is released as LGPL3 and is linked dynamically. See LICENSE.bin.


### Test ###

#### Unit tests ####

To run all unit tests:

    make test-unit


#### System tests ####

There are system test that import and update OSM data and verify the database content.
You need `osmosis` to create the test PBF files.
There is a Makefile that creates all test files if necessary and then runs the test itself.

    make test

Call `make test-system` to skip the unit tests.

WARNING: It uses your local PostgreSQL database (`imposm_test_import`, `imposm_test_production` and `imposm_test_backup` schema). Change the database with the standard `PGDATABASE`, `PGHOST`, etc. environment variables.
