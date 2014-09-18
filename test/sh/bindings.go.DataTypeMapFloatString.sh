#!/bin/sh

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(float, string) v" --daemons=1 -- \
    go run test/go/DataTypeMapFloatString.go {HOST} {PORT}
