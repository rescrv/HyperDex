#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o LengthString "${HYPERDEX_SRCDIR}"/test/rust/LengthString.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/LengthString {HOST} {PORT}
