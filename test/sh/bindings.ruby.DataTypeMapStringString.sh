#!/bin/sh

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes map(string, string) v" --daemons=1 -- \
    ruby "${HYPERDEX_SRCDIR}"/test/ruby/DataTypeMapStringString.rb {HOST} {PORT}
