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

res = c.search('kv', {'k' : 'k1'})
assertEquals(c.get('kv', 'k1')['v'], Document({'a' : 1, 'b' : 2, 'c' : 3}))
