import os

template = '''#!/bin/sh
SPACE="{space} create {daemons} partitions tolerate {ft} failures"
exec python "${{HYPERDEX_SRCDIR}}"/test/runner.py --daemons={daemons} --space="${{SPACE}}" -- \\
     "${{HYPERDEX_BUILDDIR}}"/test/replication-stress-test --quiet -n {daemons} -h {{HOST}} -p {{PORT}}
'''

SIMPLE = "space replication key int A attributes int B, int C subspace B subspace C"
REVERSE = "space replication key int A attributes int B, int C subspace C subspace B"
COMPOSITE = "space replication key int A attributes int B, int C subspace B, C"

configs = [("simple", SIMPLE, 1, 0),
           ("simple", SIMPLE, 8, 0),
           ("simple", SIMPLE, 8, 1),
           ("simple", SIMPLE, 8, 2),
           ("reverse", REVERSE, 1, 0),
           ("reverse", REVERSE, 8, 0),
           ("reverse", REVERSE, 8, 1),
           ("reverse", REVERSE, 8, 2),
           ("composite", COMPOSITE, 1, 0),
           ("composite", COMPOSITE, 8, 0),
           ("composite", COMPOSITE, 8, 1),
           ("composite", COMPOSITE, 8, 2),
           ]

for name, space, daemons, ft in configs:
    fname = 'test/sh/replication.{name}.daemons={daemons}.fault-tolerance={ft}.sh'.format(name=name, daemons=daemons, ft=ft)
    f = open(fname, 'w')
    f.write(template.format(space=space, daemons=daemons, ft=ft))
    f.flush()
    f.close()
    os.chmod(fname, 0755)
