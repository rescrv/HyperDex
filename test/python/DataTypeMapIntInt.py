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
assertTrue(c.put('kv', 'k', {'v': {1: 7, 2: 8, 3: 9}}))
assertEquals(c.get('kv', 'k'), {'v': {1: 7, 2: 8, 3: 9}})
assertTrue(c.put('kv', 'k', {'v': {}}))
assertEquals(c.get('kv', 'k'), {'v': {}})
assertTrue(c.put('kv', 'k', {'v': {1: 7, 2: 8, 3: 9}}))

# Atomic Operations
assertTrue(c.map_atomic_add('kv', 'k', {'v': {1: 5}}))
assertTrue(c.put('kv', 'k', {'v': {1: 11, 2: 8, 3: 9}}))
assertEquals(c.get('kv', 'k'), {'v': {1: 7, 2: 8, 3: 9}})
assertTrue(c.map_atomic_min('kv', 'k', {'v': {3: 100}}))
assertEquals(c.get('kv', 'k'), {'v': {1: 7, 2: 8, 3: 9}})
assertTrue(c.map_atomic_min('kv', 'k', {'v': {3: 1}}))
assertEquals(c.get('kv', 'k'), {'v': {1: 7, 2: 8, 3: 1}})
assertTrue(c.map_atomic_max('kv', 'k', {'v': {3: 100}}))
assertEquals(c.get('kv', 'k'), {'v': {1: 7, 2: 8, 3: 100}})
