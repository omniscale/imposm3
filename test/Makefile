.PHONY: all test clean files

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
	osmosis --read-xml $< --sort type="TypeThenId" --write-pbf $@ omitmetadata=true

build/%.osc.gz: %.osc
	@mkdir -p build
	gzip --stdout $< > $@

files: $(PBF_FILES) $(OSCGZ_FILES)

test: files
	(cd .. && go test ./test $(TESTOPTS))

route_relation: files
	(cd .. && go test -test.run TestRouteRelation_ ./test $(TESTOPTS))
