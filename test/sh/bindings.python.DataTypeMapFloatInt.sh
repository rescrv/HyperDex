#!/bin/sh

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(float, int) v" --daemons=1 -- \
    python2 "${HYPERDEX_SRCDIR}"/test/python/DataTypeMapFloatInt.py {HOST} {PORT}
