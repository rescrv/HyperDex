#!/usr/bin/env python
import sys
import hyperdex.client
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))
assert c.put('kv', 'k', {}) == True
assert c.get('kv', 'k') == {'v': set([])}
assert c.put('kv', 'k', {'v': set([0.25, 1.0, 3.14])}) == True
assert c.get('kv', 'k') == {'v': set([0.25, 1.0, 3.14])}
assert c.put('kv', 'k', {'v': set([])}) == True
assert c.get('kv', 'k') == {'v': set([])}
