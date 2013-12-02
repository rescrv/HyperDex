#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes v" --daemons=1 -- \
    python "${HYPERDEX_SRCDIR}"/test/python/DataTypeString.py {HOST} {PORT}
