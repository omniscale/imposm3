.PHONY: build all test clean

IMPOSM_BIN=../imposm3

ifdef VERBOSE
NOSEOPTS = -vs
else
NOSEOPTS = -v
endif

all: build test

build:
	cd ..; make build

$(IMPOSM_BIN): build

clean:
	rm -rf build

build/%.pbf: %.osm
	@mkdir -p build
	osmosis --read-xml $< --write-pbf $@ omitmetadata=true

build/%.osc.gz: %.osc
	@mkdir -p build
	gzip --stdout $< > $@

test: .lasttestrun_complete_db .lasttestrun_single_table

.lasttestrun_complete_db: $(IMPOSM_BIN) complete_db_test.py build/complete_db.osc.gz build/complete_db.pbf
	nosetests complete_db_test.py $(NOSEOPTS)
	@touch .lasttestrun_complete_db

.lasttestrun_single_table: $(IMPOSM_BIN) single_table_test.py build/single_table.osc.gz build/single_table.pbf
	nosetests single_table_test.py $(NOSEOPTS)
	@touch .lasttestrun_single_table