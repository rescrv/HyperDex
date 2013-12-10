#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes list(string) v" --daemons=1 -- \
    python "${HYPERDEX_SRCDIR}"/test/python/DataTypeListString.py {HOST} {PORT}
