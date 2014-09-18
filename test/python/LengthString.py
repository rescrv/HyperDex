#!/usr/bin/env python2
import sys
import hyperdex.client
from hyperdex.client import LessEqual, GreaterEqual, LessThan, GreaterThan, Range, Regex, LengthEquals, LengthLessEqual, LengthGreaterEqual
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))
def to_objectset(xs):
    return set([frozenset(x.items()) for x in xs])
assert c.put('kv', 'A', {}) == True
assert c.put('kv', 'AB', {}) == True
assert c.put('kv', 'ABC', {}) == True
assert c.put('kv', 'ABCD', {}) == True
assert c.put('kv', 'ABCDE', {}) == True
assert to_objectset(c.search('kv', {'k': LengthEquals(1)})) == to_objectset([{'k': 'A'}])
assert to_objectset(c.search('kv', {'k': LengthEquals(2)})) == to_objectset([{'k': 'AB'}])
assert to_objectset(c.search('kv', {'k': LengthEquals(3)})) == to_objectset([{'k': 'ABC'}])
assert to_objectset(c.search('kv', {'k': LengthEquals(4)})) == to_objectset([{'k': 'ABCD'}])
assert to_objectset(c.search('kv', {'k': LengthEquals(5)})) == to_objectset([{'k': 'ABCDE'}])
assert to_objectset(c.search('kv', {'k': LengthLessEqual(3)})) == to_objectset([{'k': 'A'}, {'k': 'AB'}, {'k': 'ABC'}])
assert to_objectset(c.search('kv', {'k': LengthGreaterEqual(3)})) == to_objectset([{'k': 'ABC'}, {'k': 'ABCD'}, {'k': 'ABCDE'}])
