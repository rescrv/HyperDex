#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes list(float) v" --daemons=1 -- \
    python "${HYPERDEX_SRCDIR}"/test/python/DataTypeListFloat.py {HOST} {PORT}
