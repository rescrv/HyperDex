#!/usr/bin/env python
import sys
import hyperdex.client
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))
assert c.put('kv', 'k', {}) == True
assert c.get('kv', 'k') == {'v': set([])}
assert c.put('kv', 'k', {'v': set(['A', 'C', 'B'])}) == True
assert c.get('kv', 'k') == {'v': set(['A', 'C', 'B'])}
assert c.put('kv', 'k', {'v': set([])}) == True
assert c.get('kv', 'k') == {'v': set([])}
