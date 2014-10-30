#!/usr/bin/env python2
import sys
import hyperdex.client
from testlib import *
from hyperdex.client import LessEqual, GreaterEqual, LessThan, GreaterThan, Range, Regex, LengthEquals, LengthLessEqual, LengthGreaterEqual

c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))
def to_objectset(xs):
    return set([frozenset(x.items()) for x in xs])
    
assertTrue(c.put('kv', 'k', {}))
assertEquals(c.get('kv', 'k'), {'v': 0})
assertTrue(c.put('kv', 'k', {'v': 1}))
assertEquals(c.get('kv', 'k'), {'v': 1})
assertTrue(c.put('kv', 'k', {'v': -1}))
assertEquals(c.get('kv', 'k'), {'v': -1})
assertTrue(c.put('kv', 'k', {'v': 0}))
assertEquals(c.get('kv', 'k'), {'v': 0})
assertTrue(c.put('kv', 'k', {'v': 9223372036854775807}))
assertEquals(c.get('kv', 'k'), {'v': 9223372036854775807})
assertTrue(c.put('kv', 'k', {'v': -9223372036854775808}))
assertEquals(c.get('kv', 'k'), {'v': -9223372036854775808})

# Atomic Operations
assertTrue(c.put('kv', 'k', {'v': -1}))
assertEquals(c.get('kv', 'k'), {'v': -1})
assertTrue(c.atomic_min('kv', 'k', {'v': -2}))
assertEquals(c.get('kv', 'k'), {'v': -2})
assertTrue(c.atomic_max('kv', 'k', {'v': 1}))
assertEquals(c.get('kv', 'k'), {'v': 1})
assertTrue(c.atomic_min('kv', 'k', {'v': 2}))
assertEquals(c.get('kv', 'k'), {'v': 1})
assertTrue(c.atomic_add('kv', 'k', {'v': 128}))
assertEquals(c.get('kv', 'k'), {'v': 129})
assertTrue(c.atomic_add('kv', 'k', {'v': -5}))
assertEquals(c.get('kv', 'k'), {'v': 124})
