.PHONY: all test clean

ifdef VERBOSE
TESTOPTS = -v
else
TESTOPTS =
endif

all: test

clean:
	rm -rf build

PBF_FILES=$(addprefix build/,$(patsubst %.osm,%.pbf,$(wildcard *.osm)))
OSCGZ_FILES=$(addprefix build/,$(patsubst %.osc,%.osc.gz,$(wildcard *.osc)))

build/%.pbf: %.osm
	@mkdir -p build
	osmosis --read-xml $< --sort TypeThenId --write-pbf $@ omitmetadata=false

build/%.osc.gz: %.osc
	@mkdir -p build
	gzip --stdout $< > $@

test: $(PBF_FILES) $(OSCGZ_FILES)
	(cd .. && godep go test ./test $(TESTOPTS))
