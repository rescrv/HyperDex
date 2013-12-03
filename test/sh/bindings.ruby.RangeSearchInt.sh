#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key int k attribute int v primary_index v create 4 partitions" --daemons=1 -- \
    ruby "${HYPERDEX_SRCDIR}"/test/ruby/RangeSearchInt.rb {HOST} {PORT}
