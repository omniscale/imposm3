#!/bin/bash
set -x -e
pushd ..
go build
popd
osmosis --read-xml ./test.osm --write-pbf ./test.pbf omitmetadata=true
gzip --stdout ./test.osc > ./test.osc.gz