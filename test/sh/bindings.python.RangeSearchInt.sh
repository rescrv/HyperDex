#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key int k attribute int v" --daemons=1 -- \
    python "${HYPERDEX_SRCDIR}"/test/python/RangeSearchInt.py {HOST} {PORT}
