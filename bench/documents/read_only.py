#! /usr/bin/python

import sys
from hyperdex.mongo import HyperDatabase, HyperSpace 

# Setup
db = HyperDatabase(sys.argv[1], int(sys.argv[2]))
db.space.insert('val', {'this': {'is' : { 'just' : { 'a' : { 'nested' : 'document' }}}}})

# Run 
db.space.get('val')
