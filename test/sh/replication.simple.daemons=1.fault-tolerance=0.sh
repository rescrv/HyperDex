#!/bin/sh
SPACE="space replication key int A attributes int B, int C subspace B subspace C create 1 partitions tolerate 0 failures"
exec python2 "${HYPERDEX_SRCDIR}"/test/runner.py --daemons=1 --space="${SPACE}" -- \
     "${HYPERDEX_BUILDDIR}"/test/replication-stress-test --quiet -n 1 -h {HOST} -p {PORT}
