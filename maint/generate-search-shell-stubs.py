import os

template = '''#!/bin/sh
SPACE="{space} create {daemons} partitions tolerate {ft} failures"
exec python test/runner.py --daemons={daemons} --space="${{SPACE}}" -- \\
     {{PATH}}/test/search-stress-test --quiet -h {{HOST}} -p {{PORT}} -k {keytype}
'''

SIMPLE = '''space search key {keytype} number attributes
    bit01, bit02, bit03, bit04, bit05, bit06, bit07, bit08,
    bit09, bit10, bit11, bit12, bit13, bit14, bit15, bit16,
    bit17, bit18, bit19, bit20, bit21, bit22, bit23, bit24,
    bit25, bit26, bit27, bit28, bit29, bit30, bit31, bit32'''
INDEX='''space search key {keytype} number attributes
    bit01, bit02, bit03, bit04, bit05, bit06, bit07, bit08,
    bit09, bit10, bit11, bit12, bit13, bit14, bit15, bit16,
    bit17, bit18, bit19, bit20, bit21, bit22, bit23, bit24,
    bit25, bit26, bit27, bit28, bit29, bit30, bit31, bit32
    primary_index
    bit01, bit02, bit03, bit04, bit05, bit06, bit07, bit08,
    bit09, bit10, bit11, bit12, bit13, bit14, bit15, bit16,
    bit17, bit18, bit19, bit20, bit21, bit22, bit23, bit24,
    bit25, bit26, bit27, bit28, bit29, bit30, bit31, bit32'''
SUBSPACE='''space search key {keytype} number attributes
    bit01, bit02, bit03, bit04, bit05, bit06, bit07, bit08,
    bit09, bit10, bit11, bit12, bit13, bit14, bit15, bit16,
    bit17, bit18, bit19, bit20, bit21, bit22, bit23, bit24,
    bit25, bit26, bit27, bit28, bit29, bit30, bit31, bit32
    subspace bit01, bit02, bit03, bit04, bit05, bit06, bit07, bit08
    subspace bit09, bit10, bit11, bit12, bit13, bit14, bit15, bit16
    subspace bit17, bit18, bit19, bit20, bit21, bit22, bit23, bit24
    subspace bit25, bit26, bit27, bit28, bit29, bit30, bit31, bit32'''
COMBINATION='''space search key {keytype} number attributes
    bit01, bit02, bit03, bit04, bit05, bit06, bit07, bit08,
    bit09, bit10, bit11, bit12, bit13, bit14, bit15, bit16,
    bit17, bit18, bit19, bit20, bit21, bit22, bit23, bit24,
    bit25, bit26, bit27, bit28, bit29, bit30, bit31, bit32
    subspace bit01, bit02, bit03, bit04, bit05, bit06, bit07, bit08
    secondary_index bit09, bit10, bit11, bit12, bit13, bit14, bit15, bit16
    subspace bit09, bit10, bit11, bit12, bit13, bit14, bit15, bit16
    secondary_index bit17, bit18, bit19, bit20, bit21, bit22, bit23, bit24
    subspace bit17, bit18, bit19, bit20, bit21, bit22, bit23, bit24
    secondary_index bit25, bit26, bit27, bit28, bit29, bit30, bit31, bit32
    subspace bit25, bit26, bit27, bit28, bit29, bit30, bit31, bit32
    secondary_index bit01, bit02, bit03, bit04, bit05, bit06, bit07, bit08'''

configs = [("simple", SIMPLE.format(keytype='int'), 'int', 1, 0),
           ("simple", SIMPLE.format(keytype='int'), 'int', 8, 0),
           ("simple", SIMPLE.format(keytype='int'), 'int', 8, 1),
           ("simple", SIMPLE.format(keytype='string'), 'string', 1, 0),
           ("simple", SIMPLE.format(keytype='string'), 'string', 8, 0),
           ("simple", SIMPLE.format(keytype='string'), 'string', 8, 1),
           ("index", INDEX.format(keytype='int'), 'int', 1, 0),
           ("index", INDEX.format(keytype='int'), 'int', 8, 0),
           ("index", INDEX.format(keytype='int'), 'int', 8, 1),
           ("index", INDEX.format(keytype='string'), 'string', 1, 0),
           ("index", INDEX.format(keytype='string'), 'string', 8, 0),
           ("index", INDEX.format(keytype='string'), 'string', 8, 1),
           ("subspace", SUBSPACE.format(keytype='int'), 'int', 1, 0),
           ("subspace", SUBSPACE.format(keytype='int'), 'int', 8, 0),
           ("subspace", SUBSPACE.format(keytype='int'), 'int', 8, 1),
           ("subspace", SUBSPACE.format(keytype='string'), 'string', 1, 0),
           ("subspace", SUBSPACE.format(keytype='string'), 'string', 8, 0),
           ("subspace", SUBSPACE.format(keytype='string'), 'string', 8, 1),
           ("combination", COMBINATION.format(keytype='int'), 'int', 1, 0),
           ("combination", COMBINATION.format(keytype='int'), 'int', 8, 0),
           ("combination", COMBINATION.format(keytype='int'), 'int', 8, 1),
           ("combination", COMBINATION.format(keytype='string'), 'string', 1, 0),
           ("combination", COMBINATION.format(keytype='string'), 'string', 8, 0),
           ("combination", COMBINATION.format(keytype='string'), 'string', 8, 1),
          ]

for name, space, keytype, daemons, ft in configs:
    fname = 'test/sh/search.{name}.keytype={keytype},daemons={daemons}.fault-tolerance={ft}.sh'.format(name=name, keytype=keytype, daemons=daemons, ft=ft)
    f = open(fname, 'w')
    f.write(template.format(space=space.replace('\n', ' '), keytype=keytype, daemons=daemons, ft=ft))
    f.flush()
    f.close()
    os.chmod(fname, 0755)
