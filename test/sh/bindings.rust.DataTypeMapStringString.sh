#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o DataTypeMapStringString "${HYPERDEX_SRCDIR}"/test/rust/DataTypeMapStringString.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(string, string) v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/DataTypeMapStringString {HOST} {PORT}
