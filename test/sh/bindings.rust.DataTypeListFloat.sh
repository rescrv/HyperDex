#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o DataTypeListFloat "${HYPERDEX_SRCDIR}"/test/rust/DataTypeListFloat.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes list(float) v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/DataTypeListFloat {HOST} {PORT}
