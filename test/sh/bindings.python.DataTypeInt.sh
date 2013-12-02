#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes int v" --daemons=1 -- \
    python "${HYPERDEX_SRCDIR}"/test/python/DataTypeInt.py {HOST} {PORT}
