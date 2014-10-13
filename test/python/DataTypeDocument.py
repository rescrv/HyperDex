#!/usr/bin/env python2
import sys
import hyperdex.client
from hyperdex.client import LessEqual, GreaterEqual, LessThan, GreaterThan, Range, Regex, LengthEquals, LengthLessEqual, LengthGreaterEqual
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))

def to_objectset(xs):
    return set([frozenset(x.items()) for x in xs])

def assertEquals(actual, expected):
	if not actual == expected:
		print "AssertEquals failed"
		print "Should be: " + str(expected) + ", but was " + str(actual) + "."

		assert False

Document = hyperdex.client.Document

assert c.put('kv', 'k', {'v': Document({})}) == True
assertEquals(c.get('kv', 'k')['v'], Document({}))
assert c.put('kv', 'k',  {'v': Document({'a': 'b', 'c': {'d' : 1, 'e': 'f', 'g': -2 }})}) == True
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'b', 'c': {'d' : 1, 'e': 'f', 'g': -2 }}))
assert c.document_atomic_add('kv', 'k',  {'v': Document({'a': 1})}) == False
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'b', 'c': {'d' : 1, 'e': 'f', 'g': -2 }}))
assert c.document_atomic_add('kv', 'k',  {'v': Document({'c': {'d' : 5}})}) == True
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'b', 'c': {'d' : 6, 'e': 'f', 'g': -2 }}))
assert c.document_atomic_add('kv', 'k',  {'v': Document({'c': {'d' : 5, 'g': 5}})}) == True
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'b', 'c': {'d' : 11, 'e': 'f' , 'g': 3}}))
assert c.document_string_prepend('kv', 'k',  {'v': Document({'a': 'x', 'c' : {'e': 'z'}})}) == True
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'xb', 'c': {'d' : 11, 'e': 'zf', 'g': 3}}))
assert c.document_string_append('kv', 'k',  {'v': Document({'a': 'x', 'c' : {'e': 'z'}})}) == True
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'xbx', 'c': {'d' : 11, 'e': 'zfz', 'g': 3}}))
