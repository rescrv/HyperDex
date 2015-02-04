#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o DataTypeMapStringFloat "${HYPERDEX_SRCDIR}"/test/rust/DataTypeMapStringFloat.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(string, float) v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/DataTypeMapStringFloat {HOST} {PORT}
