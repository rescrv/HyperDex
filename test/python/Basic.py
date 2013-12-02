#!/usr/bin/env python
import sys
import hyperdex.client
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))
assert c.get('kv', 'k') == None
assert c.put('kv', 'k', {'v': 'v1'}) == True
assert c.get('kv', 'k') == {'v': 'v1'}
assert c.put('kv', 'k', {'v': 'v2'}) == True
assert c.get('kv', 'k') == {'v': 'v2'}
assert c.put('kv', 'k', {'v': 'v3'}) == True
assert c.get('kv', 'k') == {'v': 'v3'}
assert c.delete('kv', 'k') == True
assert c.get('kv', 'k') == None
