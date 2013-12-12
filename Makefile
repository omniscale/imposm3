.PHONY: test all build clean test test-system test-unit update_version

PROTOFILES=$(shell find . -name \*.proto)
PBGOFILES=$(patsubst %.proto,%.pb.go,$(PROTOFILES))
GOFILES=$(shell find . \( -name \*.go ! -name version.go \) )

# for protoc-gen-go
export PATH := $(GOPATH)/bin:$(PATH)

GOLDFLAGS=-ldflags '-r $${ORIGIN}/lib'


BUILD_DATE=$(shell date +%Y%m%d)
BUILD_REV=$(shell git rev-parse --short HEAD)
BUILD_VERSION=dev-$(BUILD_DATE)-$(BUILD_REV)

all: build test

update_version:
	@perl -p -i -e 's/buildVersion = ".*"/buildVersion = "$(BUILD_VERSION)"/' cmd/version.go

revert_version:
	@perl -p -i -e 's/buildVersion = ".*"/buildVersion = ""/' cmd/version.go

imposm3: $(GOFILES) $(PROTOFILES)
	$(MAKE) update_version
	go build $(GOLDFLAGS)
	$(MAKE) revert_version

build: imposm3

clean:
	rm -f imposm3
	(cd test && make clean)

test: test-unit test-system

test-unit: imposm3
	go test imposm3/... -i
	go test imposm3/...

test-system: imposm3
	(cd test && make test)

%.pb.go: %.proto
	protoc --go_out=. $^
