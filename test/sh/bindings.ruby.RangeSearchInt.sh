#!/bin/sh

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key int k attribute int v" --daemons=1 -- \
    ruby "${HYPERDEX_SRCDIR}"/test/ruby/RangeSearchInt.rb {HOST} {PORT}
