#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o DataTypeListInt "${HYPERDEX_SRCDIR}"/test/rust/DataTypeListInt.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes list(int) v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/DataTypeListInt {HOST} {PORT}
