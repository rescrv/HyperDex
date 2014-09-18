#!/bin/sh
javac -d "${HYPERDEX_BUILDDIR}"/test/java "${HYPERDEX_SRCDIR}"/test/java/MultiAttribute.java

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes v1, v2" --daemons=1 -- \
    java -ea -Djava.library.path="${HYPERDEX_BUILDDIR}"/.libs:/usr/local/lib:/usr/local/lib64:/usr/lib:/usr/lib64 MultiAttribute {HOST} {PORT}
