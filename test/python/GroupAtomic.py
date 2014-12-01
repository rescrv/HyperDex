#!/usr/bin/env python2
import sys
import hyperdex.client
import json
import os
from testlib import *
from hyperdex.client import *

c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))

assertTrue(c.put('kv', 'k1',  {'v': Document({'a': 'b', 'c': {'d' : 1, 'e': 'f', 'g': -2 }})}))
assertEquals(c.get('kv', 'k1')['v'], Document({'a': 'b', 'c': {'d' : 1, 'e': 'f', 'g': -2 }}))
assertTrue(c.put('kv', 'k2',  {'v': Document({'a': 'b', 'c': {'d' : 1, 'e': 'f', 'g': -2 }})}))
assertEquals(c.get('kv', 'k2')['v'], Document({'a': 'b', 'c': {'d' : 1, 'e': 'f', 'g': -2 }}))
assertEquals(c.count('kv', {}), 2)
assertEquals(c.group_del('kv', {'v.a' : 'b'}), 2)
assertEquals(c.count('kv', {}), 0)

assertTrue(c.put('kv', 'k',  {'v': Document({'a': 'b', 'c': {'d' : 1, 'e': 'f', 'g': -2 }})}))
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'b', 'c': {'d' : 1, 'e': 'f', 'g': -2 }}))
assertEquals(c.count('kv', {}), 1)

# Group Atomic on a single document
assertEquals(c.group_atomic_add('kv', {'k':'wrong_key'},  {'v.a': 1}), 0)
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'b', 'c': {'d' : 1, 'e': 'f', 'g': -2 }}))
assertEquals(c.group_atomic_add('kv', {'k':'k'},  {'v.c.d': 1}), 1)
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'b', 'c': {'d' : 2, 'e': 'f', 'g': -2 }}))
assertEquals(c.group_atomic_add('kv', {'k':'k'},  {'v.e': 1}), 1)
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'b', 'c': {'d' : 2, 'e': 'f', 'g': -2 }, 'e' : 1}))
assertEquals(c.group_atomic_add('kv', {'v.c.d': 2},  {'v.c.g': 5}), 1)
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'b', 'c': {'d' : 2, 'e': 'f', 'g': 3 }, 'e' : 1}))
assertEquals(c.group_put('kv', {'v.a' : 'b'}, {'v.f': 42}), 1)
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'b', 'c': {'d' : 2, 'e': 'f', 'g': 3 }, 'e' : 1, 'f' : 42}))
assertTrue(c.delete('kv',  'k'))

# Group atomic on two documents (the actual interesting stuff)
assertTrue(c.put('kv', 'k1',  {'v': Document({'a': 'b', 'c': {'d' : 1, 'e': 'f', 'g': -2 }})}))
assertEquals(c.get('kv', 'k1')['v'], Document({'a': 'b', 'c': {'d' : 1, 'e': 'f', 'g': -2 }}))
assertTrue(c.put('kv', 'k2',  {'v': Document({'a': 'b', 'c': {'d' : 1, 'e': 'f', 'g': -2 }})}))
assertEquals(c.get('kv', 'k2')['v'], Document({'a': 'b', 'c': {'d' : 1, 'e': 'f', 'g': -2 }}))
assertEquals(c.group_atomic_add('kv', {'v.a':'b'},  {'v.c.d': 20}), 2)
assertEquals(c.get('kv', 'k1')['v'], Document({'a': 'b', 'c': {'d' : 21, 'e': 'f', 'g': -2 }}))
assertEquals(c.get('kv', 'k2')['v'], Document({'a': 'b', 'c': {'d' : 21, 'e': 'f', 'g': -2 }}))
assertEquals(c.group_atomic_mul('kv', {'v.a':'b'},  {'v.c.d': 2.5}), 2)
assertEquals(c.get('kv', 'k1')['v'], Document({'a': 'b', 'c': {'d' : 52.5, 'e': 'f', 'g': -2 }}))
assertEquals(c.get('kv', 'k2')['v'], Document({'a': 'b', 'c': {'d' : 52.5, 'e': 'f', 'g': -2 }}))
assertEquals(c.group_atomic_div('kv', {'v.a':'b'},  {'v.c.d': -21}), 2)
assertEquals(c.get('kv', 'k1')['v'], Document({'a': 'b', 'c': {'d' : -2.5, 'e': 'f', 'g': -2 }}))
assertEquals(c.get('kv', 'k2')['v'], Document({'a': 'b', 'c': {'d' : -2.5, 'e': 'f', 'g': -2 }}))

