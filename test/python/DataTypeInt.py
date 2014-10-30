#!/usr/bin/env python2
import sys
import hyperdex.client
from hyperdex.client import LessEqual, GreaterEqual, LessThan, GreaterThan, Range, Regex, LengthEquals, LengthLessEqual, LengthGreaterEqual
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))
def to_objectset(xs):
    return set([frozenset(x.items()) for x in xs])
assert c.put('kv', 'k', {}) == True
assert c.get('kv', 'k') == {'v': 0}
assert c.put('kv', 'k', {'v': 1}) == True
assert c.get('kv', 'k') == {'v': 1}
assert c.put('kv', 'k', {'v': -1}) == True
assert c.get('kv', 'k') == {'v': -1}
assert c.put('kv', 'k', {'v': 0}) == True
assert c.get('kv', 'k') == {'v': 0}
assert c.put('kv', 'k', {'v': 9223372036854775807}) == True
assert c.get('kv', 'k') == {'v': 9223372036854775807}
assert c.put('kv', 'k', {'v': -9223372036854775808}) == True
assert c.get('kv', 'k') == {'v': -9223372036854775808}
