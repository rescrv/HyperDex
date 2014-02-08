export HYPERDEX_SRCDIR="$1"
export HYPERDEX_BUILDDIR="$2"
export HYPERDEX_VERSION="$3"

export CLASSPATH="${HYPERDEX_BUILDDIR}"/bindings/java/org.hyperdex.client-${HYPERDEX_VERSION}.jar:"${HYPERDEX_BUILDDIR}"/test/java:
export PYTHONPATH="$2"/bindings/python:"$2"/bindings/python/.libs:
export RUBYLIB="$1"/bindings/ruby/.libs:"$2"/bindings/ruby/.libs
