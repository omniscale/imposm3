.PHONY: test all build clean test test-system test-unit update_version docs

PROTOFILES=$(shell find . -name \*.proto)
PBGOFILES=$(patsubst %.proto,%.pb.go,$(PROTOFILES))
GOFILES=$(shell find . \( -name \*.go ! -name version.go \) )

# for protoc-gen-go
export PATH := $(GOPATH)/bin:$(PATH)

GOLDFLAGS=-ldflags '-r $${ORIGIN}/lib'

GO:=$(if $(shell go version |grep 'go1.5'),GO15VENDOREXPERIMENT=1,) go

BUILD_DATE=$(shell date +%Y%m%d)
BUILD_REV=$(shell git rev-parse --short HEAD)
BUILD_VERSION=dev-$(BUILD_DATE)-$(BUILD_REV)

all: build test

update_version:
	@perl -p -i -e 's/buildVersion = ".*"/buildVersion = "$(BUILD_VERSION)"/' version.go

revert_version:
	@perl -p -i -e 's/buildVersion = ".*"/buildVersion = ""/' version.go

imposm3: $(PBGOFILES) $(GOFILES)
	$(MAKE) update_version
	$(GO) build $(GOLDFLAGS) ./cmd/imposm3
	$(MAKE) revert_version

build: imposm3

clean:
	rm -f imposm3
	(cd test && make clean)

test: imposm3 system-test-files
	$(GO) test ./... -i
	$(GO) test `$(GO) list ./... | grep -Ev '/vendor'`

test-unit: imposm3
	$(GO) test ./... -i
	$(GO) test `$(GO) list ./... | grep -Ev '/test|/vendor'`

test-system: imposm3
	(cd test && make test)

system-test-files:
	(cd test && make files)

%.pb.go: %.proto
	protoc --go_out=. $^

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