#!/bin/sh

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes int v" --daemons=1 -- \
    go run test/go/DataTypeInt.go {HOST} {PORT}
