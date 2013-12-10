#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes set(float) v" --daemons=1 -- \
    python "${HYPERDEX_SRCDIR}"/test/python/DataTypeSetFloat.py {HOST} {PORT}
