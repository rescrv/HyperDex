#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes set(string) v" --daemons=1 -- \
    python "${HYPERDEX_SRCDIR}"/test/python/DataTypeSetString.py {HOST} {PORT}
