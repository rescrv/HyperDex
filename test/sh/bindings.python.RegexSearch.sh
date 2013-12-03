#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k" --daemons=1 -- \
    python "${HYPERDEX_SRCDIR}"/test/python/RegexSearch.py {HOST} {PORT}
