import os

template = '''#!/bin/sh
python "${{HYPERDEX_SRCDIR}}"/test/runner.py --daemons=1 -- \\
    python "${{HYPERDEX_SRCDIR}}"/test/doctest-runner.py \\
           "${{HYPERDEX_SRCDIR}}"/test/doc.{name}.py {{HOST}} {{PORT}}
'''

for x in ('quick-start', 'data-types', 'async-ops', 'atomic-ops', 'documents'):
    fname = 'test/sh/doc.{name}.sh'.format(name=x)
    f = open(fname, 'w')
    f.write(template.format(name=x))
    f.flush()
    f.close()
    os.chmod(fname, 0755)
