#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o DataTypeSetInt "${HYPERDEX_SRCDIR}"/test/rust/DataTypeSetInt.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes set(int) v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/DataTypeSetInt {HOST} {PORT}
