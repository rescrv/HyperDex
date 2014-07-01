#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(int, string) v" --daemons=1 -- \
    go run test/go/DataTypeMapIntString.go {HOST} {PORT}
