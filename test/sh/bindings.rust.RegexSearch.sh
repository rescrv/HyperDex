#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o RegexSearch "${HYPERDEX_SRCDIR}"/test/rust/RegexSearch.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/RegexSearch {HOST} {PORT}
