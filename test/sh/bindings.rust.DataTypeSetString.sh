#!/bin/sh
rustc -g -L rust_hyperdex/target/debug -L rust_hyperdex/target/debug/deps -o "${HYPERDEX_BUILDDIR}"/test/rust/DataTypeSetString "${HYPERDEX_SRCDIR}"/test/rust/DataTypeSetString.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes set(string) v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/test/rust/DataTypeSetString {HOST} {PORT}
