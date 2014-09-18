#!/bin/sh

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes list(int) v" --daemons=1 -- \
    go run test/go/DataTypeListInt.go {HOST} {PORT}
