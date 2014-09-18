#!/usr/bin/env python2
import sys
import hyperdex.client
from hyperdex.client import LessEqual, GreaterEqual, LessThan, GreaterThan, Range, Regex, LengthEquals, LengthLessEqual, LengthGreaterEqual
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))
def to_objectset(xs):
    return set([frozenset(x.items()) for x in xs])
assert c.put('kv', 'k', {}) == True
assert c.get('kv', 'k') == {'v': ''}
assert c.put('kv', 'k', {'v': 'xxx'}) == True
assert c.get('kv', 'k') == {'v': 'xxx'}
assert c.put('kv', 'k', {'v': '\xde\xad\xbe\xef'}) == True
assert c.get('kv', 'k') == {'v': '\xde\xad\xbe\xef'}
