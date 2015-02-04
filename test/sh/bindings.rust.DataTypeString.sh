#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o DataTypeString "${HYPERDEX_SRCDIR}"/test/rust/DataTypeString.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/DataTypeString {HOST} {PORT}
