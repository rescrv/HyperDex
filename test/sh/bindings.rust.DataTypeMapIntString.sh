#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o DataTypeMapIntString "${HYPERDEX_SRCDIR}"/test/rust/DataTypeMapIntString.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(int, string) v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/DataTypeMapIntString {HOST} {PORT}
