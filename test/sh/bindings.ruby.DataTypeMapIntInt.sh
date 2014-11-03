#!/bin/sh

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(int, int) v" --daemons=1 -- \
    ruby "${HYPERDEX_SRCDIR}"/test/ruby/DataTypeMapIntInt.rb {HOST} {PORT}
