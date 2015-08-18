import os

template = '''#!/usr/bin/env gremlin
include 5-node-cluster

run python2 "${{HYPERDEX_SRCDIR}}"/test/doctest-runner.py "${{HYPERDEX_SRCDIR}}"/test/doc.{name}.py 127.0.0.1 1982
'''

for x in ('quick-start', 'data-types', 'async-ops', 'atomic-ops', 'documents', 'authorization'):
    fname = 'test/gremlin/doc.{name}'.format(name=x)
    f = open(fname, 'w')
    f.write(template.format(name=x))
    f.flush()
    f.close()
    os.chmod(fname, 0755)
