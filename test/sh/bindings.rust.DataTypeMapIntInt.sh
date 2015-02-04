#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o DataTypeMapIntInt "${HYPERDEX_SRCDIR}"/test/rust/DataTypeMapIntInt.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(int, int) v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/DataTypeMapIntInt {HOST} {PORT}
