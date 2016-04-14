#!/bin/sh

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attribute v" --daemons=1 -- \
    python2 "${HYPERDEX_SRCDIR}"/test/python/Basic.py {HOST} {PORT}
