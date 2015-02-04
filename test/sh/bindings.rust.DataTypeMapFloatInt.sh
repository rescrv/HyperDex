#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o DataTypeMapFloatInt "${HYPERDEX_SRCDIR}"/test/rust/DataTypeMapFloatInt.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(float, int) v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/DataTypeMapFloatInt {HOST} {PORT}
