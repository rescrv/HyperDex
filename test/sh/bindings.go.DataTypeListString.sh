#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes list(string) v" --daemons=1 -- \
    go run test/go/DataTypeListString.go {HOST} {PORT}
