#!/bin/sh

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k" --daemons=1 -- \
    python2 "${HYPERDEX_SRCDIR}"/test/python/RegexSearch.py {HOST} {PORT}
