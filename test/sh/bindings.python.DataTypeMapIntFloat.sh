#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(int, float) v" --daemons=1 -- \
    python "${HYPERDEX_SRCDIR}"/test/python/DataTypeMapIntFloat.py {HOST} {PORT}
