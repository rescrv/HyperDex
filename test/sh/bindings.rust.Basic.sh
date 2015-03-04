#!/bin/sh
rustc -g -L rust_hyperdex/target -o "${HYPERDEX_BUILDDIR}"/test/rust/Basic "${HYPERDEX_SRCDIR}"/test/rust/Basic.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attribute v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/test/rust/Basic {HOST} {PORT}
