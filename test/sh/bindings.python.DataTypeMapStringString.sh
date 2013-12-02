#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(string, string) v" --daemons=1 -- \
    python "${HYPERDEX_SRCDIR}"/test/python/DataTypeMapStringString.py {HOST} {PORT}
