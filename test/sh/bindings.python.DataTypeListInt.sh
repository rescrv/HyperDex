#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes list(int) v" --daemons=1 -- \
    python "${HYPERDEX_SRCDIR}"/test/python/DataTypeListInt.py {HOST} {PORT}
