#!/bin/sh
rustc -g -L rust_hyperdex/target -o "${HYPERDEX_BUILDDIR}"/test/rust/RangeSearchInt "${HYPERDEX_SRCDIR}"/test/rust/RangeSearchInt.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key int k attribute int v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/test/rust/RangeSearchInt {HOST} {PORT}
