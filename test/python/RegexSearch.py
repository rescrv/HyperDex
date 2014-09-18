#!/usr/bin/env python2
import sys
import hyperdex.client
from hyperdex.client import LessEqual, GreaterEqual, LessThan, GreaterThan, Range, Regex, LengthEquals, LengthLessEqual, LengthGreaterEqual
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))
def to_objectset(xs):
    return set([frozenset(x.items()) for x in xs])
assert c.put('kv', 'foo/foo/foo', {}) == True
assert c.put('kv', 'foo/foo/bar', {}) == True
assert c.put('kv', 'foo/foo/baz', {}) == True
assert c.put('kv', 'foo/bar/foo', {}) == True
assert c.put('kv', 'foo/bar/bar', {}) == True
assert c.put('kv', 'foo/bar/baz', {}) == True
assert c.put('kv', 'foo/baz/foo', {}) == True
assert c.put('kv', 'foo/baz/bar', {}) == True
assert c.put('kv', 'foo/baz/baz', {}) == True
assert c.put('kv', 'bar/foo/foo', {}) == True
assert c.put('kv', 'bar/foo/bar', {}) == True
assert c.put('kv', 'bar/foo/baz', {}) == True
assert c.put('kv', 'bar/bar/foo', {}) == True
assert c.put('kv', 'bar/bar/bar', {}) == True
assert c.put('kv', 'bar/bar/baz', {}) == True
assert c.put('kv', 'bar/baz/foo', {}) == True
assert c.put('kv', 'bar/baz/bar', {}) == True
assert c.put('kv', 'bar/baz/baz', {}) == True
assert c.put('kv', 'baz/foo/foo', {}) == True
assert c.put('kv', 'baz/foo/bar', {}) == True
assert c.put('kv', 'baz/foo/baz', {}) == True
assert c.put('kv', 'baz/bar/foo', {}) == True
assert c.put('kv', 'baz/bar/bar', {}) == True
assert c.put('kv', 'baz/bar/baz', {}) == True
assert c.put('kv', 'baz/baz/foo', {}) == True
assert c.put('kv', 'baz/baz/bar', {}) == True
assert c.put('kv', 'baz/baz/baz', {}) == True
assert to_objectset(c.search('kv', {'k': Regex('^foo')})) == to_objectset([{'k': 'foo/foo/foo'}, {'k': 'foo/foo/bar'}, {'k': 'foo/foo/baz'}, {'k': 'foo/bar/foo'}, {'k': 'foo/bar/bar'}, {'k': 'foo/bar/baz'}, {'k': 'foo/baz/foo'}, {'k': 'foo/baz/bar'}, {'k': 'foo/baz/baz'}])
assert to_objectset(c.search('kv', {'k': Regex('foo$')})) == to_objectset([{'k': 'foo/foo/foo'}, {'k': 'foo/bar/foo'}, {'k': 'foo/baz/foo'}, {'k': 'bar/foo/foo'}, {'k': 'bar/bar/foo'}, {'k': 'bar/baz/foo'}, {'k': 'baz/foo/foo'}, {'k': 'baz/bar/foo'}, {'k': 'baz/baz/foo'}])
assert to_objectset(c.search('kv', {'k': Regex('^b.*/foo/.*$')})) == to_objectset([{'k': 'bar/foo/foo'}, {'k': 'bar/foo/bar'}, {'k': 'bar/foo/baz'}, {'k': 'baz/foo/foo'}, {'k': 'baz/foo/bar'}, {'k': 'baz/foo/baz'}])
