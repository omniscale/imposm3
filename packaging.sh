#! /bin/bash

cat <<EOF
=================== Imposm Packaging Script ============================

This script creates binary packages for Imposm for Linux.
It installs and builds all dependencies, compiles the master
branch of this local repository and creates a .tar.gz with
the imposm3 binary and all 3rd party dependencies.

This script is made for Debian 10. The resulting binaries
should be compatible with all more recent Linux distribution.

'Vagrantfile' defines a working Debian VM that will call this script
during the provision phase. Please install Vagrant and Virtualbox first:
https://www.vagrantup.com/

To start the VM and the packaging:
    $ vagrant up --provision

The resulting .tar.gz should appear in dist/

To build another package, e.g. after you commited new changes:
    $ vagrant provision
or
    $ vagrant ssh
    % bash /vagrant/packaging.sh

You can specify a revision or branch by setting the REVISION
environment. REVISION defaults to the master branch:

    $ REVISION=mybranch vagrant provision

To shutdown the VM:
    $ vagrant halt

To remove the VM:
    $ vagrant destroy


EOF

set -e
# set -x

REVISION=${REVISION:-master}
if [[ -z "$IMPOSM_BUILD_RELEASE" ]]; then
    unset IMPOSM_BUILD_RELEASE
fi

BUILD_BASE=$HOME/imposm
PREFIX=$BUILD_BASE/local
SRC=$BUILD_BASE/src
export GOPATH=$BUILD_BASE/gopath
export PATH=$PATH:$BUILD_BASE/go/bin
export GOROOT=$BUILD_BASE/go
IMPOSM_SRC=$GOPATH/src/github.com/omniscale/imposm3
BUILD_TMP=$BUILD_BASE/imposm-build

GEOS_VERSION=3.12.2
GO_VERSION=1.22.4
LEVELDB_VERSION=1.23

export CGO_CFLAGS=-I$PREFIX/include
export CGO_LDFLAGS=-L$PREFIX/lib
export LD_LIBRARY_PATH=$PREFIX/lib

CURL="curl --silent --show-error --location"

mkdir -p $SRC
mkdir -p $PREFIX/lib
mkdir -p $PREFIX/include
mkdir -p $GOPATH


if ! grep --silent 'Debian GNU/Linux 10' /etc/issue; then
    echo
    echo "ERROR: This script only works for Debian 10.0 (Buster), see above."
    exit 1
fi

if [ ! -e /usr/bin/unzip ]; then
    echo "-> installing dependencies"

    sudo apt-get update -y
    sudo apt-get install -y build-essential unzip autoconf libtool git patchelf curl
fi

if [ ! -e $BUILD_BASE/go/bin/go ]; then
    echo "-> installing go"
    pushd $SRC
        $CURL https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz -O
        tar xzf go${GO_VERSION}.linux-amd64.tar.gz -C $BUILD_BASE/
    popd
fi

if [[ ! -e $PREFIX/lib/libleveldb.so ]]; then
    echo "-> installing leveldb"
    pushd $SRC
        git clone --recurse-submodules https://github.com/google/leveldb.git
        pushd leveldb
            git checkout $LEVELDB_VERSION
            mkdir -p build && cd build
            cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON -DLEVELDB_BUILD_TESTS=OFF -DLEVELDB_BUILD_BENCHMARKS=OFF .. && cmake --build .
            cp -R ./liblevel* $PREFIX/lib/
            cp -R ../include/leveldb $PREFIX/include/
        popd
    popd
fi

if [ ! -e $PREFIX/lib/libgeos.so.$GEOS_VERSION ]; then
    echo "-> installing GEOS"
    pushd $SRC
        $CURL http://download.osgeo.org/geos/geos-$GEOS_VERSION.tar.bz2 -O
        tar jxf geos-$GEOS_VERSION.tar.bz2
        pushd geos-$GEOS_VERSION/
            ./configure --prefix=$PREFIX
            make -j2
            make install
        popd
    popd
fi

echo '-> fetching imposm'
mkdir -p $IMPOSM_SRC
git init $IMPOSM_SRC

pushd $IMPOSM_SRC
    git config --add receive.denyCurrentBranch ignore

    pushd /vagrant
        git push --tags -f $IMPOSM_SRC
    popd

    git reset --hard
    git checkout $REVISION

    echo '-> compiling imposm'
    make clean
    make build
popd


echo '-> building imposm package'
rm -rf $BUILD_TMP
mkdir -p $BUILD_TMP
pushd $IMPOSM_SRC
    cp imposm $BUILD_TMP
    cp README.md $BUILD_TMP
    cp example-mapping.json $BUILD_TMP/mapping.json
popd

mkdir -p $BUILD_TMP/lib
pushd $PREFIX/lib
    cp libgeos_c.so $BUILD_TMP/lib
    ln -s libgeos_c.so $BUILD_TMP/lib/libgeos_c.so.1
    cp libgeos.so $BUILD_TMP/lib
    ln -s libgeos.so $BUILD_TMP/lib/libgeos.so.$GEOS_VERSION
    cp -R libleveldb.so* $BUILD_TMP/lib
popd

pushd $BUILD_TMP/lib
    patchelf --set-rpath '$ORIGIN' libgeos_c.so
popd


pushd $BUILD_BASE
    VERSION=`$BUILD_TMP/imposm version`-linux-x86-64
    rm -rf imposm-$VERSION
    mv imposm-build imposm-$VERSION
    tar zcvf imposm-$VERSION.tar.gz imposm-$VERSION
    mkdir -p /vagrant/dist
    mv imposm-$VERSION.tar.gz /vagrant/dist/
popd
