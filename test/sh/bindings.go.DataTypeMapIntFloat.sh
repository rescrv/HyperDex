#!/bin/sh

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(int, float) v" --daemons=1 -- \
    go run test/go/DataTypeMapIntFloat.go {HOST} {PORT}
