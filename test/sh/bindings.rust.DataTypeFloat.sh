#!/bin/sh
rustc -L rust_hyperdex/target -o "${HYPERDEX_BUILDDIR}"/test/rust/DataTypeFloat "${HYPERDEX_SRCDIR}"/test/rust/DataTypeFloat.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes float v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/test/rust/DataTypeFloat {HOST} {PORT}
