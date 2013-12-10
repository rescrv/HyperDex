#!/usr/bin/env python
import sys
import hyperdex.client
from hyperdex.client import LessEqual, GreaterEqual, Range, Regex, LengthEquals, LengthLessEqual, LengthGreaterEqual
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))
def to_objectset(xs):
    return set([frozenset(x.items()) for x in xs])
assert c.get('kv', 'k') == None
assert c.put('kv', 'k', {'v1': 'ABC'}) == True
assert c.get('kv', 'k') == {'v1': 'ABC', 'v2': ''}
assert c.put('kv', 'k', {'v2': '123'}) == True
assert c.get('kv', 'k') == {'v1': 'ABC', 'v2': '123'}
