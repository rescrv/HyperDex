#!/bin/sh
rustc -g -L rust_hyperdex/target -o "${HYPERDEX_BUILDDIR}"/test/rust/DataTypeMapFloatFloat "${HYPERDEX_SRCDIR}"/test/rust/DataTypeMapFloatFloat.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(float, float) v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/test/rust/DataTypeMapFloatFloat {HOST} {PORT}
