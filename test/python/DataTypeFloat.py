#!/usr/bin/env python
import sys
import hyperdex.client
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))
assert c.put('kv', 'k', {}) == True
assert c.get('kv', 'k') == {'v': 0.0}
assert c.put('kv', 'k', {'v': 3.14}) == True
assert c.get('kv', 'k') == {'v': 3.14}
