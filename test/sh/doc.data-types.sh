#!/bin/sh
python test/runner.py --daemons=1 -- \
    python test/doctest-runner.py test/doc.data-types.py {HOST} {PORT}
