#!/usr/bin/env python2
import sys
import hyperdex.client
from hyperdex.client import LessEqual, GreaterEqual, LessThan, GreaterThan, Range, Regex, LengthEquals, LengthLessEqual, LengthGreaterEqual
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))
def to_objectset(xs):
    return set([frozenset(x.items()) for x in xs])
assert c.put('kv', -2, {'v': -2}) == True
assert c.put('kv', -1, {'v': -1}) == True
assert c.put('kv', 0, {'v': 0}) == True
assert c.put('kv', 1, {'v': 1}) == True
assert c.put('kv', 2, {'v': 2}) == True
assert to_objectset(c.search('kv', {'k': LessEqual(0)})) == to_objectset([{'k': -2, 'v': -2}, {'k': -1, 'v': -1}, {'k': 0, 'v': 0}])
assert to_objectset(c.search('kv', {'v': LessEqual(0)})) == to_objectset([{'k': -2, 'v': -2}, {'k': -1, 'v': -1}, {'k': 0, 'v': 0}])
assert to_objectset(c.search('kv', {'k': GreaterEqual(0)})) == to_objectset([{'k': 0, 'v': 0}, {'k': 1, 'v': 1}, {'k': 2, 'v': 2}])
assert to_objectset(c.search('kv', {'v': GreaterEqual(0)})) == to_objectset([{'k': 0, 'v': 0}, {'k': 1, 'v': 1}, {'k': 2, 'v': 2}])
assert to_objectset(c.search('kv', {'k': LessThan(0)})) == to_objectset([{'k': -2, 'v': -2}, {'k': -1, 'v': -1}])
assert to_objectset(c.search('kv', {'v': LessThan(0)})) == to_objectset([{'k': -2, 'v': -2}, {'k': -1, 'v': -1}])
assert to_objectset(c.search('kv', {'k': GreaterThan(0)})) == to_objectset([{'k': 1, 'v': 1}, {'k': 2, 'v': 2}])
assert to_objectset(c.search('kv', {'v': GreaterThan(0)})) == to_objectset([{'k': 1, 'v': 1}, {'k': 2, 'v': 2}])
assert to_objectset(c.search('kv', {'k': Range(-1, 1)})) == to_objectset([{'k': -1, 'v': -1}, {'k': 0, 'v': 0}, {'k': 1, 'v': 1}])
assert to_objectset(c.search('kv', {'v': Range(-1, 1)})) == to_objectset([{'k': -1, 'v': -1}, {'k': 0, 'v': 0}, {'k': 1, 'v': 1}])
