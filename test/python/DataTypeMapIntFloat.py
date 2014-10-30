#!/usr/bin/env python2
import sys
import hyperdex.client
from testlib import *
from hyperdex.client import LessEqual, GreaterEqual, LessThan, GreaterThan, Range, Regex, LengthEquals, LengthLessEqual, LengthGreaterEqual

c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))

def to_objectset(xs):
    return set([frozenset(x.items()) for x in xs])
    
assertTrue(c.put('kv', 'k', {}))
assertEquals(c.get('kv', 'k'), {'v': {}})
assertTrue(c.put('kv', 'k', {'v': {1: 3.14, 2: 0.25, 3: 1.0}}))
assertEquals(c.get('kv', 'k'), {'v': {1: 3.14, 2: 0.25, 3: 1.0}})
assertTrue(c.put('kv', 'k', {'v': {}}))
assertEquals(c.get('kv', 'k'), {'v': {}})

#Atomic Operations
assertTrue(c.put('kv', 'k', {'v': {1: 3.14, 2: 0.25, 3: 1.0}}))
assertEquals(c.get('kv', 'k'), {'v': {1: 3.14, 2: 0.25, 3: 1.0}})
assertTrue(c.map_atomic_add('kv', 'k', {'v' : {2 : 0.5}}))
assertEquals(c.get('kv', 'k'), {'v': {1: 3.14, 2: 0.75, 3: 1.0}})
assertTrue(c.map_atomic_min('kv', 'k', {'v' : {2 : 0.5}}))
assertEquals(c.get('kv', 'k'), {'v': {1: 3.14, 2: 0.5, 3: 1.0}})
assertTrue(c.map_atomic_min('kv', 'k', {'v' : {2 : 1.5}}))
assertEquals(c.get('kv', 'k'), {'v': {1: 3.14, 2: 0.5, 3: 1.0}})
assertTrue(c.map_atomic_max('kv', 'k', {'v' : {2 : 1.5}}))
assertEquals(c.get('kv', 'k'), {'v': {1: 3.14, 2: 1.5, 3: 1.0}})

