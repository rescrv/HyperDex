#!/bin/sh
javac -cp "${HYPERDEX_SRCDIR}"/bindings/java "${HYPERDEX_SRCDIR}"/test/java/RangeSearchInt.java

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key int k attribute int v primary_index v create 4 partitions" --daemons=1 -- \
    java -Djava.library.path="${HYPERDEX_SRCDIR}"/.libs -cp "${HYPERDEX_SRCDIR}"/bindings/java:"${HYPERDEX_SRCDIR}"/test/java RangeSearchInt {HOST} {PORT}
