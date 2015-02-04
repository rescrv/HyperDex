#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o BasicSearch "${HYPERDEX_SRCDIR}"/test/rust/BasicSearch.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attribute v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/BasicSearch {HOST} {PORT}
