#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(int, string) v" --daemons=1 -- \
    python "${HYPERDEX_SRCDIR}"/test/python/DataTypeMapIntString.py {HOST} {PORT}
