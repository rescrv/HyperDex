#!/bin/sh

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(int, int) v" --daemons=1 -- \
    go run test/go/DataTypeMapIntInt.go {HOST} {PORT}
