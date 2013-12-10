#!/bin/sh

python "${HYPERDEX_SRCDIR}"/test/runner.py --space="space kv key k" --daemons=1 -- \
    ruby "${HYPERDEX_SRCDIR}"/test/ruby/RegexSearch.rb {HOST} {PORT}
