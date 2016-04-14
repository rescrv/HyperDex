#!/bin/sh

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(float, float) v" --daemons=1 -- \
    go run test/go/DataTypeMapFloatFloat.go {HOST} {PORT}
