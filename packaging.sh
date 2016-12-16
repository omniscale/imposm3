#! /bin/bash

cat <<EOF
=================== Imposm Packaging Script ============================

This script creates binary packages for Imposm 3 for Linux.
It installs and builds all dependencies, compiles the master
branch of this local repository and creates a .tar.gz with
the imposm3 binary and all 3rd party dependencies.

This script is made for Debian 6, so that the resulting binaries
are compatible with older Linux distributions, namely SLES 11, RHEL 6,
Ubuntu 10.04 and Debian 6.

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

REVISION=${1:-master}

BUILD_BASE=$HOME/imposm
PREFIX=$BUILD_BASE/local
SRC=$BUILD_BASE/src
export GOPATH=$BUILD_BASE/gopath
export PATH=$PATH:$BUILD_BASE/go/bin
export GOROOT=$BUILD_BASE/go
IMPOSM_SRC=$GOPATH/src/github.com/omniscale/imposm3
BUILD_TMP=$BUILD_BASE/imposm-build

GEOS_VERSION=3.5.1

export CGO_CFLAGS=-I$PREFIX/include
export CGO_LDFLAGS=-L$PREFIX/lib
export LD_LIBRARY_PATH=$PREFIX/lib

CURL="curl --silent --show-error --location"

mkdir -p $SRC
mkdir -p $PREFIX
mkdir -p $GOPATH


if ! grep --silent 'Debian GNU/Linux 6.0' /etc/issue; then
    echo
    echo "ERROR: This script only works for Debian 6.0 (Squeeze), see above."
    exit 1
fi

if [ ! -e /usr/bin/git ]; then
    echo "-> installing dependencies"

    # squeeze is EOL, use debian-archive
    cat <<EOF | sudo tee /etc/apt/sources.list
deb http://ftp.de.debian.org/debian-archive/debian squeeze main
deb-src http://ftp.de.debian.org/debian-archive/debian squeeze main
EOF

    sudo apt-get update -y
    sudo apt-get install -y build-essential unzip autoconf libtool git-core chrpath
fi

if [ ! -e $BUILD_BASE/go/bin/go ]; then
    echo "-> installing go"
    pushd $SRC
        $CURL https://storage.googleapis.com/golang/go1.7.3.linux-amd64.tar.gz -O
        tar xzf go1.7.3.linux-amd64.tar.gz -C $BUILD_BASE/
    popd
fi

if [ ! -e $PREFIX/lib/libhyperleveldb.so ]; then
    echo "-> installing hyperleveldb"
    pushd $SRC
        $CURL https://github.com/rescrv/HyperLevelDB/archive/master.zip -O
        unzip master.zip
        pushd HyperLevelDB-master
            autoreconf -i
            ./configure --prefix=$PREFIX
            make -j4
            make install
        popd
    popd $SRC
fi

if [ ! -e $PREFIX/include/leveldb ]; then
    echo "-> linking hyperleveldb as leveldb"
    pushd $PREFIX/lib
        for s in 'a', 'la', 'so'; do
            ln -sf libhyperleveldb.$s libleveldb.$s
        done
    popd
    ln -s $PREFIX/include/hyperleveldb $PREFIX/include/leveldb
fi

if [ ! -e $PREFIX/lib/libprotobuf.so ]; then
    echo "-> installing protobuf"
    pushd $SRC
        $CURL https://github.com/google/protobuf/releases/download/v2.6.1/protobuf-2.6.1.tar.bz2 -O
        tar jxf protobuf-2.6.1.tar.bz2
        pushd protobuf-2.6.1/
            ./configure --prefix=$PREFIX
            make -j2
            make install
        popd
    popd
fi

if [ ! -e $PREFIX/lib/libgeos-$GEOS_VERSION.so ]; then
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
        git push --all -f $IMPOSM_SRC
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
    cp imposm3 $BUILD_TMP
    cp example-mapping.json $BUILD_TMP/mapping.json
popd

mkdir -p $BUILD_TMP/lib
pushd $PREFIX/lib
    cp libgeos_c.so $BUILD_TMP/lib
    ln -s libgeos_c.so $BUILD_TMP/lib/libgeos_c.so.1
    cp libgeos.so $BUILD_TMP/lib
    ln -s libgeos.so $BUILD_TMP/lib/libgeos-$GEOS_VERSION.so
    cp libhyperleveldb.so $BUILD_TMP/lib
    ln -s libhyperleveldb.so $BUILD_TMP/lib/libhyperleveldb.so.0
    ln -s libhyperleveldb.so $BUILD_TMP/lib/libleveldb.so.1
popd

pushd $BUILD_TMP/lib
    chrpath libgeos_c.so -r '${ORIGIN}'
popd


pushd $BUILD_BASE
    VERSION=`$BUILD_TMP/imposm3 version`-linux-x86-64
    rm -rf imposm3-$VERSION
    mv imposm-build imposm3-$VERSION
    tar zcvf imposm3-$VERSION.tar.gz imposm3-$VERSION
    mkdir -p /vagrant/dist
    mv imposm3-$VERSION.tar.gz /vagrant/dist/
    echo "placed final package in: ./dist/imposm3-$VERSION.tar.gz"
popd
