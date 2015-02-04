#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o DataTypeSetString "${HYPERDEX_SRCDIR}"/test/rust/DataTypeSetString.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes set(string) v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/DataTypeSetString {HOST} {PORT}
