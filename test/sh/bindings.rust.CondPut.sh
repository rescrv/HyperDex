#!/bin/sh
rustc -g -L rust_hyperdex/target/debug -L rust_hyperdex/target/debug/deps -o "${HYPERDEX_BUILDDIR}"/test/rust/CondPut "${HYPERDEX_SRCDIR}"/test/rust/CondPut.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attribute v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/test/rust/CondPut {HOST} {PORT}
