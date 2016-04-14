#!/bin/sh
SPACE="space replication key int A attributes int B, int C subspace C subspace B create 4 partitions tolerate 0 failures"
exec python2 "${HYPERDEX_SRCDIR}"/test/runner.py --daemons=4 --space="${SPACE}" -- \
     "${HYPERDEX_BUILDDIR}"/test/replication-stress-test --quiet -n 4 -h {HOST} -p {PORT}
