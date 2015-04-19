#!/bin/sh
rustc -g -L rust_hyperdex/target/debug -L rust_hyperdex/target/debug/deps -o "${HYPERDEX_BUILDDIR}"/test/rust/MultiAttribute "${HYPERDEX_SRCDIR}"/test/rust/MultiAttribute.rs

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes v1, v2" --daemons=1 -- \
    "${HYPERDEX_BUILDDIR}"/test/rust/MultiAttribute {HOST} {PORT}
