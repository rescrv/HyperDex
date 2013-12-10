#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attribute v" --daemons=1 -- \
    python "${HYPERDEX_SRCDIR}"/test/python/RangeSearchString.py {HOST} {PORT}
