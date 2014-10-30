#!/usr/bin/env python2
import sys
import hyperdex.client
from testlib import *

from hyperdex.client import LessEqual, GreaterEqual, LessThan, GreaterThan, Range, Regex, LengthEquals, LengthLessEqual, LengthGreaterEqual
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))

def to_objectset(xs):
    return set([frozenset(x.items()) for x in xs])

assertTrue(c.put('kv', 'k', {}))
assertEquals(c.get('kv', 'k'), {'v': 0.0})
assertTrue(c.put('kv', 'k', {'v': 3.14}))
assertEquals(c.get('kv', 'k'), {'v': 3.14})

# Atomic Operations
assertTrue(c.put('kv', 'k', {'v': 13.37}))
assertEquals(c.get('kv', 'k'), {'v': 13.37})
assertTrue(c.atomic_add('kv', 'k', {'v': 1.5}))
assertEquals(c.get('kv', 'k'), {'v': 14.87})
assertTrue(c.atomic_add('kv', 'k', {'v': -15.0}))
assertEquals(c.get('kv', 'k'), {'v': -0.13})
