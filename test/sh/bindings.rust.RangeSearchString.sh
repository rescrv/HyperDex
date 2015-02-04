#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o RangeSearchString "${HYPERDEX_SRCDIR}"/test/rust/RangeSearchString.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attribute v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/RangeSearchString {HOST} {PORT}
