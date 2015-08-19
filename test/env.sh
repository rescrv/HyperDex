export HYPERDEX_SRCDIR="$1"
export HYPERDEX_BUILDDIR="$2"
export HYPERDEX_VERSION="$3"

export HYPERDEX_EXEC_PATH="${HYPERDEX_BUILDDIR}"
export HYPERDEX_COORD_LIB="${HYPERDEX_BUILDDIR}"/.libs/libhyperdex-coordinator

export PATH=${HYPERDEX_BUILDDIR}:${HYPERDEX_SRCDIR}:${PATH}

export CLASSPATH="${HYPERDEX_BUILDDIR}"/bindings/java/org.hyperdex.client-${HYPERDEX_VERSION}.jar:"${HYPERDEX_BUILDDIR}"/test/java:${CLASSPATH}
export PYTHONPATH="$2"/bindings/python:"$2"/bindings/python/.libs:${PYTHONPATH}
export RUBYLIB="$1"/bindings/ruby/.libs:"$2"/bindings/ruby/.libs:${RUBYLIB}
