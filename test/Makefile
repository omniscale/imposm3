.PHONY: build all test clean

IMPOSM_BIN=../imposm3

all: build test

build:
	cd ..; make build

$(IMPOSM_BIN): build

clean:
	rm -rf build

build/test.pbf: test.osm
	@mkdir -p build
	osmosis --read-xml ./test.osm --write-pbf ./build/test.pbf omitmetadata=true

build/test.osc.gz: test.osc
	@mkdir -p build
	gzip --stdout ./test.osc > ./build/test.osc.gz

test: .lasttestrun

.lasttestrun: $(IMPOSM_BIN) imposm_system_test.py build/test.osc.gz build/test.pbf
	nosetests imposm_system_test.py -v
	@touch .lasttestrun