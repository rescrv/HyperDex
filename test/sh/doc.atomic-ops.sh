#!/bin/sh
python test/runner.py --daemons=1 -- \
    python test/doctest-runner.py test/doc.atomic-ops.py {HOST} {PORT}
