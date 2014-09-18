#!/usr/bin/env python2
import sys
import hyperdex.client
from hyperdex.client import LessEqual, GreaterEqual, LessThan, GreaterThan, Range, Regex, LengthEquals, LengthLessEqual, LengthGreaterEqual
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))
def to_objectset(xs):
    return set([frozenset(x.items()) for x in xs])
assert c.get('kv', 'k') == None
assert c.put('kv', 'k', {'v': 'v1'}) == True
assert c.get('kv', 'k') == {'v': 'v1'}
assert c.cond_put('kv', 'k', {'v': 'v2'}, {'v': 'v3'}) == False
assert c.get('kv', 'k') == {'v': 'v1'}
assert c.cond_put('kv', 'k', {'v': 'v1'}, {'v': 'v3'}) == True
assert c.get('kv', 'k') == {'v': 'v3'}
