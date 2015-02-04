#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o DataTypeMapStringInt "${HYPERDEX_SRCDIR}"/test/rust/DataTypeMapStringInt.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(string, int) v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/DataTypeMapStringInt {HOST} {PORT}
