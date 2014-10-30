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
assertTrue(c.put('kv', 'k', {'v': {1: 'X', 2: 'Y', 3: 'Z'}}))
assertEquals(c.get('kv', 'k'), {'v': {1: 'X', 2: 'Y', 3: 'Z'}})
assertTrue(c.put('kv', 'k', {'v': {}}))
assertEquals(c.get('kv', 'k'), {'v': {}})

#Atomic Operations
assertTrue(c.put('kv', 'k', {'v': {1: 'X', 2: 'Y', 3: 'Z'}}))
assertEquals(c.get('kv', 'k'), {'v': {1: 'X', 2: 'Y', 3: 'Z'}})
assertTrue(c.map_string_append('kv', 'k', {'v' : {3 : 'A'}}))
assertEquals(c.get('kv', 'k'), {'v': {1: 'X', 2: 'Y', 3: 'ZA'}})
assertTrue(c.map_string_prepend('kv', 'k', {'v' : {2 : 'U', 3 : 'B'}}))
assertEquals(c.get('kv', 'k'), {'v': {1: 'X', 2: 'UY', 3: 'BZA'}})
