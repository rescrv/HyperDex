#! /usr/bin/python

import sys
import functools
import time

from functools import partial
from benchmark import Benchmark
from bulk_insert import BulkInserter
from hyperdex.mongo import HyperDatabase, HyperSpace 

### CONFIG ###
# Modify this to change the behaviour of the benchmark

# How many atomic add calls?
numCalls = 1000
# How many documents in the space ? 
numDocs = 100000
# Gaps between calling 
addGap = 1
##############

# Verify Config
assert numCalls > 0
assert numDocs > 0
assert addGap > 0
assert (numCalls * addGap) < numDocs

# Setup
db = HyperDatabase(sys.argv[1], int(sys.argv[2]))
assert addGap > 0
assert (numCalls * addGap) < numDocs

# Setup
db = HyperDatabase(sys.argv[1], int(sys.argv[2]))

def create_doc(num_elems, prefix, pos):
        id = prefix + str(pos)
        doc = {'_id' : id}

        for i in range(num_elems):
                doc['elem' + str(i)] = i

        return doc

# Run
for n in range(1000, 10100, 1000):
        space = db.bench
    
        generator = partial(create_doc, n, 'myval')
        bulk_inserter = BulkInserter(numDocs, space, generator)
        bulk_inserter.run()

        pendingOps = []

        bench = Benchmark()
        bench.start()

        for i in range(numCalls):
                p = space.async_atomic_add('myval' + str(i*addGap), {'elem50' : 10})
                pendingOps.append(p)

        while len(pendingOps) > 0:
                pendingOps[0].wait()
                pendingOps.pop(0)

        elapsed = bench.stop()

        # Print simple CSV
        print str(n) + ',' + str(elapsed)

        space.clear()
        time.sleep(5)

