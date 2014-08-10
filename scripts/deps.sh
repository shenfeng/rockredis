#! /bin/bash

set -e
set -u

is_linux() { [ $OSTYPE = 'linux-gnu' ]; }

function full_path() {
    if is_linux; then
        readlink -f ../../../..
    else
        greadlink -f ../../../..
    fi
}

DIR=$(full_path)
export GOPATH=$DIR

mkdir -p deps
cd "$DIR/deps"
if [ -d rocksdb ]; then
     (cd rocksdb && git pull)
else
     git clone git@github.com:facebook/rocksdb.git
fi

(cd rocksdb && make -j shared_lib)

cd $DIR

CGO_CFLAGS="-I$DIR/deps/rocksdb/include" CGO_LDFLAGS="-L$DIR/deps/rocksdb/" go get github.com/tecbot/gorocksdb
