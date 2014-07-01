#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(float, int) v" --daemons=1 -- \
    go run test/go/DataTypeMapFloatInt.go {HOST} {PORT}
