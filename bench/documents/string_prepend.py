#! /usr/bin/python

import sys
import functools

from benchmark import Benchmark
from hyperdex.mongo import HyperDatabase, HyperSpace 

def createDoc(id, numElems):
        doc = {'_id' : id}

        for i in range(numElems):
                doc['elem' + str(i)] = "a"

        return doc

# Setup
db = HyperDatabase(sys.argv[1], int(sys.argv[2]))

# Run
for n in range(500, 10000, 250):
        db.bench.insert(createDoc('myval', n))
        numCalls = 1000
        pendingOps = []

        bench = Benchmark()
        bench.start()
        
        for i in range(numCalls):
                p = db.bench.async_string_prepend('myval', {'elem50' : 'b'})
                pendingOps.append(p)
        
        while len(pendingOps) > 0:
                pendingOps[0].wait()
                pendingOps.pop(0)

        elapsed = bench.stop()

        # Print simple CSV
        print str(n) + ',' + str(elapsed)

        db.bench.remove('myval')
