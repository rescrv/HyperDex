#!/usr/bin/env python2
import sys
import hyperdex.client
import json
import os
from testlib import *
from hyperdex.client import *

c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))

assertTrue(c.put('kv', 'k1', {'v' : Document({'a' : 1})}))
tx = c.microtransaction_init('kv')
tx.put({'v.b' : 2})
tx.put({'v.c' : 3})
tx.commit('k1')
assertEquals(c.get('kv', 'k1')['v'], Document({'a' : 1, 'b' : 2, 'c' : 3}))

assertTrue(c.put('kv', 'k1', {'v' : Document({})}))
tx = c.microtransaction_init('kv')
tx.put({'v.b' : 2})
tx.put({'v.c' : Document({'x' : 'y'})})
tx.commit('k1')
assertEquals(c.get('kv', 'k1')['v'], Document({'b' : 2, 'c' : {'x' : 'y'}}))

assertTrue(c.put('kv', 'k2', {'v' : Document({'a' : 1})}))
tx = c.microtransaction_init('kv')
tx.atomic_add({'v.a' : 2})
tx.atomic_add({'v.b' : -1})
tx.atomic_add({'v.c' : 3})
tx.put({'v.d' : Document({'x' : 'y'})})
assertTrue(tx.commit('k2'))
assertEquals(c.get('kv', 'k2')['v'], Document({'a' : 3, 'b' : -1, 'c' : 3, 'd' : {'x' : 'y'}}))

tx = c.microtransaction_init('kv')
tx.atomic_div({'v.a' : 3})
tx.atomic_div({'v.b' : -1})
tx.atomic_mul({'v.c' : 0})
tx.document_rename({'v.d': 'v.e'})
tx = c.microtransaction_init('kv')
assertTrue(tx.commit('k2')) 
assertEquals(c.get('kv', 'k2')['v'], Document({'a' : 3, 'b' : -1, 'c' : 3, 'd' : {'x' : 'y'}})) #rename should fail...

tx = c.microtransaction_init('kv')
tx.atomic_div({'v.a' : 3})
tx.atomic_div({'v.b' : -1})
tx.atomic_mul({'v.c' : 0})
assertTrue(tx.commit('k2'))
assertEquals(c.get('kv', 'k2')['v'], Document({'a' : 1, 'b' : 1, 'c' : 0, 'd' : {'x' : 'y'}}))

## Group commit
assertTrue(c.put('kv', 'k1', {'v' : Document({'a' : 2})}))
assertTrue(c.put('kv', 'k2', {'v' : Document({'a' : 1})}))
assertTrue(c.put('kv', 'k3', {'v' : Document({'a' : 1})}))
tx = c.microtransaction_init('kv')
tx.atomic_add({'v.a' : 2})
tx.atomic_add({'v.b' : -1})
tx.atomic_add({'v.c' : 3})
tx.put({'v.d' : Document({'x' : 'y'})})
assertEquals(tx.group_commit({'v.a' : 1}), 2)
assertEquals(c.get('kv', 'k1')['v'], Document({'a' : 2}))
assertEquals(c.get('kv', 'k2')['v'], Document({'a' : 3, 'b' : -1, 'c' : 3, 'd' : {'x' : 'y'}}))
assertEquals(c.get('kv', 'k3')['v'], Document({'a' : 3, 'b' : -1, 'c' : 3, 'd' : {'x' : 'y'}}))

## Cond commit
assertTrue(c.put('kv', 'k1', {'v' : Document({'a' : 2})}))

tx = c.microtransaction_init('kv')
tx.atomic_add({'v.a' : 2})
tx.atomic_add({'v.b' : -1})
tx.atomic_add({'v.c' : 3})
tx.put({'v.d' : Document({'x' : 'y'})})
assertFalse(tx.cond_commit('k1', {'v.a' : 1}))
assertEquals(c.get('kv', 'k1')['v'], Document({'a' : 2}))

tx = c.microtransaction_init('kv')
tx.atomic_add({'v.a' : 2})
tx.atomic_add({'v.b' : -1})
tx.atomic_add({'v.c' : 3})
tx.put({'v.d' : Document({'x' : 'y'})})
assertTrue(tx.cond_commit('k1', {'v.a' : 2}))
assertEquals(c.get('kv', 'k1')['v'], Document({'a' : 4, 'b' : -1, 'c' : 3, 'd' : {'x' : 'y'}}))





