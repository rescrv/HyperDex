#!/bin/bash

set -e

TARBALL_PO6=libpo6-0.9.dev
TARBALL_E=libe-0.12.dev
TARBALL_BUSYBEE=busybee-0.7.dev
TARBALL_LEVELDB=hyperleveldb-1.2.2
TARBALL_REPLICANT=replicant-0.8.dev
TARBALL_MACAROONS=libmacaroons-0.3.0
TARBALL_TREADSTONE=libtreadstone-0.3.dev
TARBALL_HYPERDEX=hyperdex-1.8.dev

for tarball in \
    ${TARBALL_PO6} ${TARBALL_E} ${TARBALL_BUSYBEE} \
    ${TARBALL_LEVELDB} ${TARBALL_REPLICANT} ${TARBALL_MACAROONS} \
    ${TARBALL_TREADSTONE} ${TARBALL_HYPERDEX}
do
    tar xzvf ${tarball}.tar.gz
    pushd ${tarball}
    ./configure
    make
    make install
    popd
done
