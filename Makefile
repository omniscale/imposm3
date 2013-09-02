.PHONY: test all build clean

PROTOFILES=$(shell find . -name \*.proto)
PBGOFILES=$(patsubst %.proto,%.pb.go,$(PROTOFILES))
GOFILES=$(shell find . -name \*.go)

# for protoc-gen-go
export PATH := $(GOPATH)/bin:$(PATH)

GOLDFLAGS=-ldflags '-r ${ORIGIN}:${ORIGIN}/../lib'

all: build test

imposm3: $(GOFILES) $(PROTOFILES)
	go build $(GOLDFLAGS)

build: imposm3

clean:
	rm -f imposm3
	(cd test && make clean)

test:
	(cd test && make test)

%.pb.go: %.proto
	protoc --go_out=. $^
