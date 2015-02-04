#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o DataTypeFloat "${HYPERDEX_SRCDIR}"/test/rust/DataTypeFloat.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes float v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/DataTypeFloat {HOST} {PORT}
