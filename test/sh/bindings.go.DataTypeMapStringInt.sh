#!/bin/sh

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(string, int) v" --daemons=1 -- \
    go run test/go/DataTypeMapStringInt.go {HOST} {PORT}
