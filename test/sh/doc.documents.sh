#!/bin/sh
python2 "${HYPERDEX_SRCDIR}"/test/runner.py --daemons=1 -- \
    python2 "${HYPERDEX_SRCDIR}"/test/doctest-runner.py \
           "${HYPERDEX_SRCDIR}"/test/doc.documents.py {HOST} {PORT}
