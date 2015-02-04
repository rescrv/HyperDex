#!/bin/sh
rustc --out-dir "${HYPERDEX_BUILDDIR}"/test/rust -o MultiAttribute "${HYPERDEX_SRCDIR}"/test/rust/MultiAttribute.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes v1, v2" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/MultiAttribute {HOST} {PORT}
