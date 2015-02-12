#!/bin/sh
rustc -L rust_hyperdex/target -o "${HYPERDEX_BUILDDIR}"/test/rust/DataTypeInt "${HYPERDEX_SRCDIR}"/test/rust/DataTypeInt.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes int v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/test/rust/DataTypeInt {HOST} {PORT}
