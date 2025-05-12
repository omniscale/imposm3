# Dockerfile to build Imposm releases for Linux
# ---------------------------------------------
#
# It installs and builds all dependencies, compiles the master
# branch of this local repository and creates a .tar.gz with
# the imposm binary and all 3rd party dependencies.
#
# This script is made for Debian 10. The resulting binaries
# should be compatible with all more recent Linux distribution.
#
# Release tar.gz is copied to /imposm/dist, use mounts to get the file.
# Example:
#  podman build --platform=linux/amd64 -t imposm-build-debian10 .
#  podman run --platform=linux/amd64 --rm -v ./dist:/imposm/dist -t imposm-build-debian10

FROM debian:10

RUN apt-get update && \
    apt-get install -y \
    build-essential \
    unzip \
    autoconf \
    libtool \
    git \
    cmake \
    patchelf \
    curl \
    && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /imposm

ARG PREFIX=/imposm/local

ENV CGO_CFLAGS=-I$PREFIX/include
ENV CGO_LDFLAGS=-L$PREFIX/lib
ENV LD_LIBRARY_PATH=$PREFIX/lib

RUN mkdir -p $PREFIX/lib && mkdir -p $PREFIX/include

ARG GOLANG_VERSION=1.24.0
RUN curl -L -O https://go.dev/dl/go$GOLANG_VERSION.linux-amd64.tar.gz && \
    rm -rf /usr/local/go && tar -C /usr/local -xzf go$GOLANG_VERSION.linux-amd64.tar.gz && \
    rm go$GOLANG_VERSION.linux-amd64.tar.gz
ENV PATH="$PATH:/usr/local/go/bin"


ARG LEVELDB_VERSION=1.23
RUN curl -L https://github.com/google/leveldb/archive/refs/tags/$LEVELDB_VERSION.tar.gz | \
        tar -xz && \
    cd leveldb-$LEVELDB_VERSION && \
    mkdir -p build && cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON -DLEVELDB_BUILD_TESTS=OFF -DLEVELDB_BUILD_BENCHMARKS=OFF .. && \
    cmake --build . && \
    cp -R ./liblevel* $PREFIX/lib/ && \
    cp -R ../include/leveldb $PREFIX/include/


ARG GEOS_VERSION=3.12.2
RUN curl -L http://download.osgeo.org/geos/geos-$GEOS_VERSION.tar.bz2 | tar -jx && \
    cd geos-$GEOS_VERSION/ && \
    ./configure --prefix=$PREFIX && make -j2 && make install

RUN mkdir src && git config --global --add safe.directory src
COPY . src

RUN cd src && make build

RUN mkdir -p build \ && \
    cp src/imposm build/ && \
    cp src/README.md build/ && \
    cp src/example-mapping.json build/mapping.json && \
    mkdir -p build/lib && \
    cp $PREFIX/lib/libgeos_c.so build/lib && \
    ln -s libgeos_c.so build/lib/libgeos_c.so.1 && \
    cp $PREFIX/lib/libgeos.so build/lib && \
    ln -s libgeos.so build/lib/libgeos.so.$GEOS_VERSION && \
    cp -R $PREFIX/lib/libleveldb.so* build/lib && \
    patchelf --set-rpath '$ORIGIN' build/lib/libgeos_c.so


CMD bash -c 'cd /imposm && VERSION=$(./build/imposm version)-linux-x86-64 && mv build imposm-$VERSION && tar zcvf dist/imposm-$VERSION.tar.gz imposm-$VERSION'

