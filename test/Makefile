.PHONY: build all test clean

IMPOSM_BIN=../imposm3 ../imposm3_parsemetadata

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
	osmosis --read-xml $< --sort TypeThenId --write-pbf $@ omitmetadata=true

build/%.osc.gz: %.osc
	@mkdir -p build
	gzip --stdout $< > $@

test: .lasttestrun_complete_db .lasttestrun_single_table .lasttestrun_parsemetadata_yes .lasttestrun_parsemetadata_no

.lasttestrun_complete_db: $(IMPOSM_BIN) complete_db_test.py build/complete_db.osc.gz build/complete_db.pbf complete_db_mapping.json
	nosetests complete_db_test.py $(NOSEOPTS)
	@touch .lasttestrun_complete_db

.lasttestrun_single_table: $(IMPOSM_BIN) single_table_test.py build/single_table.osc.gz build/single_table.pbf single_table_mapping.json
	nosetests single_table_test.py $(NOSEOPTS)
	@touch .lasttestrun_single_table

.lasttestrun_parsemetadata_yes: $(IMPOSM_BIN) parsemetadata_yes.py build/parsemetadata_data.osc.gz build/parsemetadata_data.pbf parsemetadata_mapping.json
	rm -rf ./build/parsemetadata_data_withmeta.pbf
	osmosis -q --read-xml ./parsemetadata_data.osm --write-pbf ./build/parsemetadata_data_withmeta.pbf omitmetadata=false
	nosetests parsemetadata_yes.py $(NOSEOPTS)
	@touch .lasttestrun_parsemetadata_yes

.lasttestrun_parsemetadata_no: $(IMPOSM_BIN) parsemetadata_no.py  build/parsemetadata_data.osc.gz build/parsemetadata_data.pbf parsemetadata_mapping.json
	rm -rf ./build/parsemetadata_data_withmeta.pbf
	osmosis -q --read-xml ./parsemetadata_data.osm --write-pbf ./build/parsemetadata_data_withmeta.pbf omitmetadata=false
	nosetests parsemetadata_no.py  $(NOSEOPTS)
	@touch .lasttestrun_parsemetadata_no


	
	
	
