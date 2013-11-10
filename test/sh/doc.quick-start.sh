#!/bin/sh
python test/runner.py --daemons=1 -- \
    python test/doctest-runner.py test/doc.quick-start.py {HOST} {PORT}
