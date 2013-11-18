#!/bin/sh
python "${HYPERDEX_SRCDIR}"/test/runner.py --daemons=1 -- \
    python "${HYPERDEX_SRCDIR}"/test/doctest-runner.py \
           "${HYPERDEX_SRCDIR}"/test/doc.data-types.py {HOST} {PORT}
