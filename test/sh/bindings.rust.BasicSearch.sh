#!/bin/sh
rustc -L rust_hyperdex/target -o "${HYPERDEX_BUILDDIR}"/test/rust/BasicSearch "${HYPERDEX_SRCDIR}"/test/rust/BasicSearch.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attribute v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/test/rust/BasicSearch {HOST} {PORT}
