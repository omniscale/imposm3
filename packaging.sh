#! /bin/bash

cat <<EOF
=================== Imposm Packaging Script ============================

This script creates binary packages for Imposm 3 for Linux.
It installs and builds all dependencies, compiles the master
branch of this local repository and creates a .tar.gz with
the imposm3 binary and all 3rd party dependencies.

This script is made for Debian 8. The resulting binaries
are compatible with Ubuntu 14.04, SLES 12, Fedora 21.

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
IMPOSM_SRC=$GOPATH/src/github.com/gregtzar/imposm3
BUILD_TMP=$BUILD_BASE/imposm-build

GEOS_VERSION=3.6.2

# If set, build with HyperLevelDB instead of LevelDB
#WITH_HYPERLEVELDB=1

export CGO_CFLAGS=-I$PREFIX/include
export CGO_LDFLAGS=-L$PREFIX/lib
export LD_LIBRARY_PATH=$PREFIX/lib

CURL="curl --silent --show-error --location"

mkdir -p $SRC
mkdir -p $PREFIX/lib
mkdir -p $PREFIX/include
mkdir -p $GOPATH


if ! grep --silent 'Debian GNU/Linux 8' /etc/issue; then
    echo
    echo "ERROR: This script only works for Debian 8.0 (Jessie), see above."
    exit 1
fi

if [ ! -e /usr/bin/git ]; then
    echo "-> installing dependencies"

    sudo apt-get update -y
    sudo apt-get install -y build-essential unzip autoconf libtool git chrpath curl
fi

if [ ! -e $BUILD_BASE/go/bin/go ]; then
    echo "-> installing go"
    pushd $SRC
        $CURL https://storage.googleapis.com/golang/go1.9.2.linux-amd64.tar.gz -O
        tar xzf go1.9.2.linux-amd64.tar.gz -C $BUILD_BASE/
    popd
fi

if [[ -z "$WITH_HYPERLEVELDB" && ! -e $PREFIX/lib/libleveldb.so ]]; then
    echo "-> installing leveldb"
    pushd $SRC
        $CURL https://github.com/google/leveldb/archive/master.zip -L -O
        unzip master.zip
        pushd leveldb-master
            make -j4
            cp -R out-shared/liblevel* $PREFIX/lib/
            cp -R include/leveldb $PREFIX/include/
        popd
    popd 
fi

if [[ -n "$WITH_HYPERLEVELDB" && ! -e $PREFIX/lib/libhyperleveldb.so ]]; then
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
    popd
fi

if [[ -n "$WITH_HYPERLEVELDB" && ! -e $PREFIX/include/leveldb ]]; then
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
    if [[ -n "$WITH_HYPERLEVELDB" ]]; then
        make build
    else
        LEVELDB_POST_121=1 make build
    fi
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
    if [ -n "$WITH_HYPERLEVELDB" ]; then
        cp libhyperleveldb.so $BUILD_TMP/lib
        ln -s libhyperleveldb.so $BUILD_TMP/lib/libhyperleveldb.so.0
        ln -s libhyperleveldb.so $BUILD_TMP/lib/libleveldb.so.1
    else
        cp -R libleveldb.so* $BUILD_TMP/lib
    fi
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

    echo "###########################################################################"
    echo " Call the following commands to download the created binary packages:"
    echo
    echo "vagrant ssh-config > .vagrant_ssh_conf"
    echo "rsync -a -v -P -e 'ssh -F .vagrant_ssh_conf' default:/vagrant/dist ./dist"
    echo "###########################################################################"
popd
