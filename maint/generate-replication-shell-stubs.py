import os

template = '''#!/usr/bin/env gremlin
include {daemons}-node-cluster-no-mt

run "${{HYPERDEX_SRCDIR}}"/test/add-space 127.0.0.1 1982 "{space} create {daemons} partitions tolerate {ft} failures"
run sleep 1
run "${{HYPERDEX_BUILDDIR}}"/test/replication-stress-test -n {daemons} -h 127.0.0.1 -p 1982
'''

SIMPLE = "space replication key int A attributes int B, int C subspace B subspace C"
REVERSE = "space replication key int A attributes int B, int C subspace C subspace B"
COMPOSITE = "space replication key int A attributes int B, int C subspace B, C"

configs = [("simple", SIMPLE, 1, 0),
           ("simple", SIMPLE, 4, 0),
           ("simple", SIMPLE, 4, 1),
           ("simple", SIMPLE, 4, 2),
           ("reverse", REVERSE, 1, 0),
           ("reverse", REVERSE, 4, 0),
           ("reverse", REVERSE, 4, 1),
           ("reverse", REVERSE, 4, 2),
           ("composite", COMPOSITE, 1, 0),
           ("composite", COMPOSITE, 4, 0),
           ("composite", COMPOSITE, 4, 1),
           ("composite", COMPOSITE, 4, 2),
           ]

for name, space, daemons, ft in configs:
    fname = 'test/gremlin/replication.{name}.daemons={daemons}.fault-tolerance={ft}'.format(name=name, daemons=daemons, ft=ft)
    f = open(fname, 'w')
    f.write(template.format(space=space, daemons=daemons, ft=ft))
    f.flush()
    f.close()
    os.chmod(fname, 0755)
