#!/bin/sh
rustc -g -L rust_hyperdex/target/debug -L rust_hyperdex/target/debug/deps -o "${HYPERDEX_BUILDDIR}"/test/rust/LengthString "${HYPERDEX_SRCDIR}"/test/rust/LengthString.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/test/rust/LengthString {HOST} {PORT}
