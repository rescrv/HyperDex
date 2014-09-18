#!/usr/bin/env python2
import sys
import hyperdex.client
from hyperdex.client import LessEqual, GreaterEqual, LessThan, GreaterThan, Range, Regex, LengthEquals, LengthLessEqual, LengthGreaterEqual
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))
def to_objectset(xs):
    return set([frozenset(x.items()) for x in xs])
assert c.put('kv', 'k', {}) == True
assert c.get('kv', 'k') == {'v': {}}
assert c.put('kv', 'k', {'v': {0.25: 'Y', 1.0: 'Z', 3.14: 'X'}}) == True
assert c.get('kv', 'k') == {'v': {0.25: 'Y', 1.0: 'Z', 3.14: 'X'}}
assert c.put('kv', 'k', {'v': {}}) == True
assert c.get('kv', 'k') == {'v': {}}
