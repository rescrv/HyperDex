#!/bin/sh
rustc -L rust_hyperdex/target -o "${HYPERDEX_BUILDDIR}"/test/rust/DataTypeMapStringFloat "${HYPERDEX_SRCDIR}"/test/rust/DataTypeMapStringFloat.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(string, float) v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/test/rust/DataTypeMapStringFloat {HOST} {PORT}
