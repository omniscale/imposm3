Imposm 3
========

Imposm is an importer for OpenStreetMap data. It reads PBF files and
imports the data into PostgreSQL/PostGIS or SpatiaLite. It can also update the
DB from diff files.

It is designed to create databases that are optimized for rendering (i.e. generating tiles or for WMS services).

Imposm 3 is written in Go and it is a complete rewrite of the previous Python implementation.
Configurations/mappings and cache files are not compatible with Imposm 2, but they share a similar architecture.


It is released as open source under the [Apache License 2.0][].

[Apache License 2.0]: http://www.apache.org/licenses/LICENSE-2.0.html


The development of Imposm 3 was sponsored by [Omniscale](http://omniscale.com/) and development will continue as resources permit.
Please get in touch if you need commercial support or if you need specific features.


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
  Limit imported geometries to polygons from Shapefiles or GeoJSON, for city/state/country imports.

- Easy deployment:
  Single binary with only runtime dependencies to common libs (GEOS, SQLite and LevelDB)

- Support for table namespace (PostgreSQL schema)


Performance
-----------

Imposm 3 is much faster than Imposm 2 and osm2pgsql:

* Makes full use of all available CPU cores
* Bulk inserts into PostgreSQL with `COPY FROM`
* Efficient intermediate cache for reduced IO load during ways and relations building


Some import times from a Hetzner EX 4S server (Intel i7-2600 CPU @ 3.40GHz, 32GB RAM and 2TB software RAID1 (2x2TB 7200rpm SATA disks)) for imports of a 20.5GB planet PBF (2013-06-14) with generalized tables:

* 6:30h in normal-mode
* 13h in diff-mode

osm2pgsql required between 2-8 days in a [similar benchmark (slide 7)](http://www.geofabrik.de/media/2012-09-08-osm2pgsql-performance.pdf) with a smaller planet PBF file (~15GB).

Benchmarks with SSD are TBD.

Import of Europe 11GB PBF with generalized tables:

* 2:20h in normal-mode


Current status
--------------

Imposm 3 is in alpha stadium and there is no official release yet.
The import itself is working however and it was already used for production databases.

### Missing ###

Compared to Imposm 2:

* Documentation
* Support for other projections than EPSG:3857
* Import of XML files

Other missing features:

* Updating generalized tables in diff-mode
* Automatic download of diff files
* Tile expire list for re-rendering updated areas
* Background mode for diff-import (update DB in background)
* Diff import into custom PG schemas
* Improve parallelization of diff import

Installation
------------

### Binary

There are no official releases, but you find development builds at <http://imposm.org/static/rel/>.
These builds are for x86 64bit Linux and require *no* further depedecies. Download, untar and start `imposm3`.
(Note: These binaries require glibc >= 2.15 at the moment.
Ubuntu 12.04 is recent enough, Debian 7 not. Future binary releases will work on older versions as well.)

### Source

There are some dependencies:

#### Compiler

You need [Go >=1.1](http://golang.org).

#### C/C++ libraries

Other dependencies are [libleveldb][], [libgeos][] and [libsqlite3][].
Imposm 3 was tested with recent versions of these libraries, but you might succeed with older versions.
For SpatiaLite support, a HEAD version of [libspatialite] is required.
GEOS >=3.2 is recommended, since it became much more robust when handling invalid geometries.
For best performance use [HyperLevelDB][libhyperleveldb] as an in-place replacement for libleveldb.


[libleveldb]: https://code.google.com/p/leveldb/
[libhyperleveldb]: https://github.com/rescrv/HyperLevelDB
[libgeos]: http://trac.osgeo.org/geos/
[libsqlite3]: http://www.sqlite.org/
[libspatialite]: https://www.gaia-gis.it/fossil/libspatialite/index

##### Building SpatiaLite

fossil clone https://www.gaia-gis.it/fossil/libspatialite libspatialite.fossil
mkdir libspatialite
cd libspatialite
fossil open ../libspatialite.fossil
./configure --prefix=/path/to/lib --enable-lwgeom=yes
make install

SpatiaLite is only needed if one wants to import into a SpatiaLite database.

#### Go libraries

Imposm3 uses the following libraries. `go get` will fetch these:

- <https://github.com/jmhodges/levigo>
- <https://github.com/mattn/go-sqlite3>
- <https://code.google.com/p/goprotobuf/proto>
- <https://code.google.com/p/goprotobuf/protoc-gen-go>
- <https://github.com/lib/pq>

For now you need to upgrade lib/pq to the bulk branch:

    cd $GOPATH/src/github.com/lib/pq
    git remote add olt https://github.com/olt/libpq.git
    git fetch olt
    git checkout olt/bulk


#### Other

Fetching Imposm and the Go libraries requires [mercurial][] and [git][].

[mercurial]: http://mercurial.selenic.com/
[git]: http://git-scm.com/


#### Compile

Create a new [Go workspace](http://golang.org/doc/code.html):

    mkdir imposm
    cd imposm
    export GOPATH=`pwd`

Get Imposm 3 and all dependencies:

    git clone https://github.com/omniscale/imposm3 src/imposm3
    go get imposm3
    go install imposm3

Done. You should now have an imposm3 binary in `$GOPATH/bin`.

Go compiles to static binaries and so Imposm 3 has no runtime dependencies to Go.
Just copy the `imposm3` binary to your server for deployment. The C/C++ libraries listed above are still required though.



Usage
-----

`imposm3` has multiple subcommands. Use `imposm3 import` for basic imports.

For a simple PostGIS import:

    imposm3 import -connection postgis://user:password@host/database \
        -mapping mapping.json -read /path/to/osm.pbf -write
		
To import into a SpatiaLite database:

    imposm3 import -connection spatialite:///path/to/foo.db \
        -mapping mapping.json -read /path/to/osm.pbf -write

You need a JSON file with the target database mapping. See `example-mapping.json` to get an idea what is possible with the mapping.

Imposm creates all new tables inside the `import` table schema. So you'll have `import.osm_roads` etc. You can change the tables to the `public` schema:

    imposm3 import -connection postgis://user:passwd@host/database \
        -mapping mapping.json -deployproduction


You can write some options into a JSON configuration file:

    {
        "cachedir": "/var/local/imposm3",
        "mapping": "mapping.json",
        "connection": "postgis://user:password@localhost:port/database"
    }

To use that config:

    imposm3 import -config config.json [args...]

For more options see:

    imposm3 import -help

Sorry, that's all documentation for the moment.


Support
-------

There is a [mailing list at Google Groups](http://groups.google.com/group/imposm) for all questions. You can subscribe by sending an email to: `imposm+subscribe@googlegroups.com`

For commercial support [contact Omniscale](http://omniscale.com/contact).

Development
-----------

The source code is available at: <https://github.com/omniscale/imposm3/>

You can report any issues at: <https://github.com/omniscale/imposm3/issues>

### Test ###

#### Unit tests ####

    go test imposm3/...


#### System tests ####

There is a system test that imports and updates OSM data and verifies the database content.
This test is written in Python and requires `nose`, `shapely` and `psycopg2`. You also need `osmosis` to create test PBF files.
There is a Makefile that (re)builds `imposm3` and creates all test files if necessary and then runs the test itself.

    make test

WARNING: It uses your local PostgeSQL database (`import` schema), if you have one. Change the database with the standard PGXXX environment variables.
