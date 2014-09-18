#!/usr/bin/env python2
import sys
import hyperdex.client
from hyperdex.client import LessEqual, GreaterEqual, LessThan, GreaterThan, Range, Regex, LengthEquals, LengthLessEqual, LengthGreaterEqual
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))
def to_objectset(xs):
    return set([frozenset(x.items()) for x in xs])
assert to_objectset(c.search('kv', {'v': 'v1'})) == to_objectset(set([]))
assert c.put('kv', 'k1', {'v': 'v1'}) == True
assert to_objectset(c.search('kv', {'v': 'v1'})) == to_objectset([{'k': 'k1', 'v': 'v1'}])
assert c.put('kv', 'k2', {'v': 'v1'}) == True
assert to_objectset(c.search('kv', {'v': 'v1'})) == to_objectset([{'k': 'k1', 'v': 'v1'}, {'k': 'k2', 'v': 'v1'}])
