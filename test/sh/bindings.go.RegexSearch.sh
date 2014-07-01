#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k" --daemons=1 -- \
    go run test/go/RegexSearch.go {HOST} {PORT}
