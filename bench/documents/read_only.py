#! /usr/bin/python

import sys
import functools

from benchmark import Benchmark
from hyperdex.mongo import HyperDatabase, HyperSpace 

def createDoc(id, numElems):
        doc = {'_id' : id}

        for i in range(numElems):
                doc['elem' + str(i)] = i

        return doc

# Setup
db = HyperDatabase(sys.argv[1], int(sys.argv[2]))

# Run
for n in range(100, 10000, 100):
        db.bench.insert(createDoc('myval', n))

        bench = Benchmark()
        result = bench.run(functools.partial(db.bench.get, 'myval'), 1000)

        print "Num Elements: " + str(n)
        print result.averageTime

        db.bench.remove('myval')
