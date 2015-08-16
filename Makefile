.PHONY: test all build clean test test-system test-unit update_version docs

PROTOFILES=$(shell find . -name \*.proto)
PBGOFILES=$(patsubst %.proto,%.pb.go,$(PROTOFILES))
GOFILES=$(shell find . \( -name \*.go ! -name version.go \) )

# for protoc-gen-go
export PATH := $(GOPATH)/bin:$(PATH)

GOLDFLAGS=-ldflags '-r $${ORIGIN}/lib'

GO=godep go

BUILD_DATE=$(shell date +%Y%m%d)
BUILD_REV=$(shell git rev-parse --short HEAD)
BUILD_VERSION=dev-$(BUILD_DATE)-$(BUILD_REV)

all: build test

update_version:
	@perl -p -i -e 's/buildVersion = ".*"/buildVersion = "$(BUILD_VERSION)"/' cmd/version.go

revert_version:
	@perl -p -i -e 's/buildVersion = ".*"/buildVersion = ""/' cmd/version.go

imposm3: $(PBGOFILES)
	$(MAKE) update_version
	$(GO) build -a $(GOLDFLAGS)
	$(GO) build -a -o 'imposm3_parsemetadata' -tags 'parsemetadata' $(GOLDFLAGS) .
	$(MAKE) revert_version

build: imposm3 imposm3_parsemetadata

clean:
	rm -f imposm3
	rm -f imposm3_parsemetadata
	(cd test && make clean)

test: test-unit test-system

test-unit: imposm3
	$(GO) test ./... -i
	$(GO) test ./...

test-system: imposm3 imposm3_parsemetadata
	(cd test && make test)

%.pb.go: %.proto
	protoc --go_out=. $^

docs:
	(cd docs && make html)

REMOTE_DOC_LOCATION = omniscale.de:domains/imposm.org/docs/imposm3
DOC_VERSION = 3.0.0

upload-docs: docs
	rsync -a -v -P -z docs/_build/html/ $(REMOTE_DOC_LOCATION)/$(DOC_VERSION)
