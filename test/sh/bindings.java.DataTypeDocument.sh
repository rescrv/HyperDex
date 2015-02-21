#!/bin/sh
javac -cp .:"${HYPERDEX_SRCDIR}"/bindings/java/:/usr/share/java/junit4.jar -d "${HYPERDEX_BUILDDIR}"/test/java "${HYPERDEX_SRCDIR}"/test/java/DataTypeDocument.java

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes document v" --daemons=1 -- \
    java -ea -Djava.library.path="${HYPERDEX_BUILDDIR}"/.libs:/usr/local/lib:/usr/local/lib64:/usr/lib:/usr/lib64:/u -Dhyperdex_host={HOST} -Dhyperdex_port={PORT} \
     -cp .:"${HYPERDEX_SRCDIR}"/bindings/java/:"${HYPERDEX_SRCDIR}"/test/java:/usr/share/java/junit4.jar org.junit.runner.JUnitCore DataTypeDocument

