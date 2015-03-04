#!/bin/sh
rustc -g -L rust_hyperdex/target -o "${HYPERDEX_BUILDDIR}"/test/rust/DataTypeListString "${HYPERDEX_SRCDIR}"/test/rust/DataTypeListString.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes list(string) v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/test/rust/DataTypeListString {HOST} {PORT}
