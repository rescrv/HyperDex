#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o RangeSearchInt "${HYPERDEX_SRCDIR}"/test/rust/RangeSearchInt.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key int k attribute int v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/RangeSearchInt {HOST} {PORT}
