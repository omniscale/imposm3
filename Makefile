.PHONY: test all build clean test test-system test-unit update_version docs

PROTOFILES=$(shell find . -name \*.proto | grep -v vendor/ )
PBGOFILES=$(patsubst %.proto,%.pb.go,$(PROTOFILES))
GOFILES=$(shell find . \( -name \*.go ! -name version.go \) | grep -v .pb.go )

# for protoc-gen-go
export PATH := $(GOPATH)/bin:$(PATH)

GOLDFLAGS=-ldflags '-r $${ORIGIN}/lib $(VERSION_LDFLAGS)'

GO:=go

ifdef LEVELDB_PRE_121
	GOTAGS=-tags="ldbpre121"
endif

BUILD_DATE=$(shell date +%Y%m%d)
BUILD_REV=$(shell git rev-parse --short HEAD)
BUILD_BRANCH=$(shell git rev-parse --abbrev-ref HEAD)
TAG=$(shell git name-rev --tags --name-only $(BUILD_REV))
ifeq ($(TAG),undefined)
	BUILD_VERSION=$(BUILD_BRANCH)-$(BUILD_DATE)-$(BUILD_REV)
else
	# use TAG but strip v of v1.2.3
	BUILD_VERSION=$(TAG:v%=%)
endif
VERSION_LDFLAGS=-X github.com/omniscale/imposm3.Version=$(BUILD_VERSION)

all: build test

imposm: $(GOFILES)
	$(GO) build $(GOTAGS) $(GOLDFLAGS) ./cmd/imposm

build: imposm

clean:
	rm -f imposm
	(cd test && make clean)

test: system-test-files
	$(GO) test $(GOTAGS) -parallel 4 ./...

test-unit: system-test-files
	$(GO) test $(GOTAGS) -test.short ./...

test-system: system-test-files
	$(GO) test $(GOTAGS) -parallel 4 ./test

system-test-files:
	(cd test && make files)

regen-protobuf: $(PBGOFILES)

%.pb.go: %.proto
	protoc --proto_path=$(GOPATH)/src:$(GOPATH)/src/github.com/omniscale/imposm3/vendor/github.com/gogo/protobuf/protobuf:. --gogofaster_out=. $^

docs:
	(cd docs && make html)

REMOTE_DOC_LOCATION = omniscale.de:/opt/www/imposm.org/docs/imposm3
DOC_VERSION = 3.0.0

upload-docs: docs
	rsync -a -v -P -z docs/_build/html/ $(REMOTE_DOC_LOCATION)/$(DOC_VERSION)


build-license-deps:
	rm LICENSE.deps
	find ./vendor -iname license\* -exec bash -c '\
		dep=$${1#./vendor/}; \
		(echo -e "========== $$dep ==========\n"; cat $$1; echo -e "\n\n") \
		| fold -s -w 80 \
		>> LICENSE.deps \
	' _ {} \;


test-coverage:
	$(GO) test -coverprofile imposm.coverprofile -coverpkg ./... -covermode count ./...
test-coverage-html: test-coverage
	$(GO) tool cover -html imposm.coverprofile

