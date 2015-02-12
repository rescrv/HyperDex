#!/bin/sh
rustc -L rust_hyperdex/target -o "${HYPERDEX_BUILDDIR}"/test/rust/DataTypeString "${HYPERDEX_SRCDIR}"/test/rust/DataTypeString.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/test/rust/DataTypeString {HOST} {PORT}
