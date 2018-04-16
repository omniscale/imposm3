.PHONY: all test clean files

ifdef VERBOSE
TESTOPTS = -v
else
TESTOPTS =
endif

all: test

clean:
	rm generated/*.{pbf,gz}

PBF_FILES=$(addprefix generated/,$(patsubst %.osm,%.pbf,$(wildcard *.osm)))
OSCGZ_FILES=$(addprefix generated/,$(patsubst %.osc,%.osc.gz,$(wildcard *.osc)))

generated/%.pbf: %.osm
	osmosis --read-xml $< --sort type="TypeThenId" --write-pbf $@ omitmetadata=true

generated/%.osc.gz: %.osc
	gzip --stdout $< > $@

files: $(PBF_FILES) $(OSCGZ_FILES)

test: files
	(cd .. && go test ./test $(TESTOPTS))

route_relation: files
	(cd .. && go test -test.run TestRouteRelation_ ./test $(TESTOPTS))
