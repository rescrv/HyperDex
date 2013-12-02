#!/usr/bin/env python
import sys
import hyperdex.client
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))
assert c.put('kv', 'k', {}) == True
assert c.get('kv', 'k') == {'v': set([])}
assert c.put('kv', 'k', {'v': set([1, 2, 3])}) == True
assert c.get('kv', 'k') == {'v': set([1, 2, 3])}
assert c.put('kv', 'k', {'v': set([])}) == True
assert c.get('kv', 'k') == {'v': set([])}
