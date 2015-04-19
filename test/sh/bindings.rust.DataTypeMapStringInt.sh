#!/bin/sh
rustc -g -L rust_hyperdex/target/debug -L rust_hyperdex/target/debug/deps -o "${HYPERDEX_BUILDDIR}"/test/rust/DataTypeMapStringInt "${HYPERDEX_SRCDIR}"/test/rust/DataTypeMapStringInt.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(string, int) v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/test/rust/DataTypeMapStringInt {HOST} {PORT}
