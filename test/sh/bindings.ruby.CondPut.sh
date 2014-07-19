#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attribute v" --daemons=1 -- \
    ruby "${HYPERDEX_SRCDIR}"/test/ruby/CondPut.rb {HOST} {PORT}
