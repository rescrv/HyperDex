#!/bin/sh

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --daemons=1 -- \
    python2 "${HYPERDEX_SRCDIR}"/test/python/Admin.py {HOST} {PORT}
