#!/usr/bin/env python2
import sys
import hyperdex.client
from hyperdex.client import LessEqual, GreaterEqual, LessThan, GreaterThan, Range, Regex, LengthEquals, LengthLessEqual, LengthGreaterEqual
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))
def to_objectset(xs):
    return set([frozenset(x.items()) for x in xs])
assert c.put('kv', 'A', {'v': 'A'}) == True
assert c.put('kv', 'B', {'v': 'B'}) == True
assert c.put('kv', 'C', {'v': 'C'}) == True
assert c.put('kv', 'D', {'v': 'D'}) == True
assert c.put('kv', 'E', {'v': 'E'}) == True
assert to_objectset(c.search('kv', {'k': LessEqual('C')})) == to_objectset([{'k': 'A', 'v': 'A'}, {'k': 'B', 'v': 'B'}, {'k': 'C', 'v': 'C'}])
assert to_objectset(c.search('kv', {'v': LessEqual('C')})) == to_objectset([{'k': 'A', 'v': 'A'}, {'k': 'B', 'v': 'B'}, {'k': 'C', 'v': 'C'}])
assert to_objectset(c.search('kv', {'k': GreaterEqual('C')})) == to_objectset([{'k': 'C', 'v': 'C'}, {'k': 'D', 'v': 'D'}, {'k': 'E', 'v': 'E'}])
assert to_objectset(c.search('kv', {'v': GreaterEqual('C')})) == to_objectset([{'k': 'C', 'v': 'C'}, {'k': 'D', 'v': 'D'}, {'k': 'E', 'v': 'E'}])
assert to_objectset(c.search('kv', {'k': LessThan('C')})) == to_objectset([{'k': 'A', 'v': 'A'}, {'k': 'B', 'v': 'B'}])
assert to_objectset(c.search('kv', {'v': LessThan('C')})) == to_objectset([{'k': 'A', 'v': 'A'}, {'k': 'B', 'v': 'B'}])
assert to_objectset(c.search('kv', {'k': GreaterThan('C')})) == to_objectset([{'k': 'D', 'v': 'D'}, {'k': 'E', 'v': 'E'}])
assert to_objectset(c.search('kv', {'v': GreaterThan('C')})) == to_objectset([{'k': 'D', 'v': 'D'}, {'k': 'E', 'v': 'E'}])
assert to_objectset(c.search('kv', {'k': Range('B', 'D')})) == to_objectset([{'k': 'B', 'v': 'B'}, {'k': 'C', 'v': 'C'}, {'k': 'D', 'v': 'D'}])
assert to_objectset(c.search('kv', {'v': Range('B', 'D')})) == to_objectset([{'k': 'B', 'v': 'B'}, {'k': 'C', 'v': 'C'}, {'k': 'D', 'v': 'D'}])
