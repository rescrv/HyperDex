#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o DataTypeInt "${HYPERDEX_SRCDIR}"/test/rust/DataTypeInt.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes int v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/DataTypeInt {HOST} {PORT}
