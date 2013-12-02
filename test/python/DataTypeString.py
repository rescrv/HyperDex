#!/usr/bin/env python
import sys
import hyperdex.client
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))
assert c.put('kv', 'k', {}) == True
assert c.get('kv', 'k') == {'v': ''}
assert c.put('kv', 'k', {'v': 'xxx'}) == True
assert c.get('kv', 'k') == {'v': 'xxx'}
assert c.put('kv', 'k', {'v': '\xde\xad\xbe\xef'}) == True
assert c.get('kv', 'k') == {'v': '\xde\xad\xbe\xef'}
