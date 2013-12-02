#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(int, int) v" --daemons=1 -- \
    python "${HYPERDEX_SRCDIR}"/test/python/DataTypeMapIntInt.py {HOST} {PORT}
