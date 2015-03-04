#!/bin/sh
rustc -g -L rust_hyperdex/target -o "${HYPERDEX_BUILDDIR}"/test/rust/RegexSearch "${HYPERDEX_SRCDIR}"/test/rust/RegexSearch.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/test/rust/RegexSearch {HOST} {PORT}
