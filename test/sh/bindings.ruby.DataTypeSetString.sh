#!/bin/sh

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes set(string) v" --daemons=1 -- \
    ruby "${HYPERDEX_SRCDIR}"/test/ruby/DataTypeSetString.rb {HOST} {PORT}
