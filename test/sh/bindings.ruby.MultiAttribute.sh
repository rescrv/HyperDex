#!/bin/sh

python2 "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k attributes v1, v2" --daemons=1 -- \
    ruby "${HYPERDEX_SRCDIR}"/test/ruby/MultiAttribute.rb {HOST} {PORT}
