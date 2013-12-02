#!/usr/bin/env python
import sys
import hyperdex.client
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))
def to_objectset(xs):
    return set([frozenset(x.items()) for x in xs])
assert c.put('kv', 'k', {}) == True
assert c.get('kv', 'k') == {'v': {}}
assert c.put('kv', 'k', {'v': {'A': 3.14, 'C': 1.0, 'B': 0.25}}) == True
assert c.get('kv', 'k') == {'v': {'A': 3.14, 'C': 1.0, 'B': 0.25}}
assert c.put('kv', 'k', {'v': {}}) == True
assert c.get('kv', 'k') == {'v': {}}
