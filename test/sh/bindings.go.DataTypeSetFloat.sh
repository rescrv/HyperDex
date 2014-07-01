#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes set(float) v" --daemons=1 -- \
    go run test/go/DataTypeSetFloat.go {HOST} {PORT}
