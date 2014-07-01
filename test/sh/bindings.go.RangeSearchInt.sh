#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key int k attribute int v" --daemons=1 -- \
    go run test/go/RangeSearchInt.go {HOST} {PORT}
