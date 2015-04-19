#!/bin/sh
rustc -g -L rust_hyperdex/target/debug -L rust_hyperdex/target/debug/deps -o "${HYPERDEX_BUILDDIR}"/test/rust/DataTypeListInt "${HYPERDEX_SRCDIR}"/test/rust/DataTypeListInt.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes list(int) v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/test/rust/DataTypeListInt {HOST} {PORT}
