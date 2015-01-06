#!/bin/bash

set -e

TARBALL_PO6=libpo6-0.5.2
TARBALL_E=libe-0.8.2
TARBALL_BUSYBEE=busybee-0.5.2
TARBALL_LEVELDB=hyperleveldb-1.2.2
TARBALL_REPLICANT=replicant-0.6.4
TARBALL_MACAROONS=libmacaroons-0.2.0
TARBALL_TREADSTONE=libtreadstone-0.1.0
TARBALL_HYPERDEX=hyperdex-1.5.0

for tarball in \
    ${TARBALL_PO6} ${TARBALL_E} ${TARBALL_BUSYBEE} \
    ${TARBALL_LEVELDB} ${TARBALL_REPLICANT} ${TARBALL_MACAROONS} \
    ${TARBALL_TREADSTONE} ${TARBALL_HYPERDEX}
do
    wget http://hyperdex.org/src/${tarball}.tar.gz
    tar xzvf ${tarball}.tar.gz
    pushd ${tarball}
    ./configure
    make
    sudo make install
    popd
done
