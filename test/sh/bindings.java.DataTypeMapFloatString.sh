#!/bin/sh
javac -d "${HYPERDEX_BUILDDIR}"/test/java "${HYPERDEX_SRCDIR}"/test/java/DataTypeMapFloatString.java

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(float, string) v" --daemons=1 -- \
    java -ea -Djava.library.path="${HYPERDEX_BUILDDIR}"/.libs:/usr/local/lib:/usr/local/lib64:/usr/lib:/usr/lib64 DataTypeMapFloatString {HOST} {PORT}
