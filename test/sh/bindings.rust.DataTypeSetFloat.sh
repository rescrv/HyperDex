#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o DataTypeSetFloat "${HYPERDEX_SRCDIR}"/test/rust/DataTypeSetFloat.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes set(float) v" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/DataTypeSetFloat {HOST} {PORT}
