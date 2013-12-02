#!/bin/sh
javac -cp "${HYPERDEX_SRCDIR}"/bindings/java "${HYPERDEX_SRCDIR}"/test/java/DataTypeMapFloatFloat.java

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(float, float) v" --daemons=1 -- \
    java -Djava.library.path="${HYPERDEX_SRCDIR}"/.libs -cp "${HYPERDEX_SRCDIR}"/bindings/java:"${HYPERDEX_SRCDIR}"/test/java DataTypeMapFloatFloat {HOST} {PORT}
