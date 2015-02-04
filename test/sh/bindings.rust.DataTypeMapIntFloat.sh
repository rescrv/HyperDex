#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o DataTypeMapIntFloat "${HYPERDEX_SRCDIR}"/test/rust/DataTypeMapIntFloat.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(int, float) v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/DataTypeMapIntFloat {HOST} {PORT}
