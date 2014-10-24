#!/usr/bin/env python2
import sys
import hyperdex.client
import json
import os
import testlib

from hyperdex.client import LessEqual, GreaterEqual, LessThan, GreaterThan, Range, Regex, LengthEquals, LengthLessEqual, LengthGreaterEqual
c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))

def to_objectset(xs):
    return set([frozenset(x.items()) for x in xs])

Document = hyperdex.client.Document
assertEquals = testlib.assertEquals

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
assert c.document_string_append('kv', 'k',  {'v': Document({'k' : {'l': 'm'}})}) == True
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'xbx', 'c': {'d' : 11, 'e': 'zfz', 'g': 3}, 'k' : {'l' : 'm'}}))
assert c.document_atomic_add('kv', 'k',  {'v': Document({'k' : {'a': {'b' : {'c' : {'d' : 1}}}}})}) == True
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'xbx', 'c': {'d' : 11, 'e': 'zfz', 'g': 3}, 'k' : {'a': {'b' : {'c' : {'d' : 1}}}, 'l' : 'm'}}))
assert c.document_atomic_sub('kv', 'k',  {'v': Document({'k' : {'a': {'b' : {'c' : {'d' : 5}}}}})}) == True
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'xbx', 'c': {'d' : 11, 'e': 'zfz', 'g': 3}, 'k' : {'a': {'b' : {'c' : {'d' : -4}}}, 'l' : 'm'}}))

# More exotic operations
assert c.put('kv', 'k3',  {'v': Document({'a': 'b', 'c': {'d' : 100, 'e': 'f', 'g': 5 }})}) == True
assertEquals(c.get('kv', 'k3')['v'], Document({'a': 'b', 'c': {'d' : 100, 'e': 'f', 'g': 5 }}))
assert c.document_atomic_mod('kv', 'k3', {'v': Document({'c' : {'d' : 10000}})})
assertEquals(c.get('kv', 'k3')['v'], Document({'a': 'b', 'c': {'d' : 100, 'e': 'f', 'g': 5 }}))
assert c.document_atomic_mod('kv', 'k3', {'v': Document({'c' : {'d' : 22}})})
assertEquals(c.get('kv', 'k3')['v'], Document({'a': 'b', 'c': {'d' : 12, 'e': 'f', 'g': 5 }}))
assert c.document_atomic_xor('kv', 'k3', {'v': Document({'c' : {'g' : 4}})})
assertEquals(c.get('kv', 'k3')['v'], Document({'a': 'b', 'c': {'d' : 12, 'e': 'f', 'g': 1 }}))
assert c.document_atomic_or('kv', 'k3', {'v': Document({'c' : {'g' : 4}})})
assertEquals(c.get('kv', 'k3')['v'], Document({'a': 'b', 'c': {'d' : 12, 'e': 'f', 'g': 5 }}))

# Multiply and divide
assert c.put('kv', 'k4',  {'v': Document({'a': 200})}) == True
assertEquals(c.get('kv', 'k4')['v'], Document({ 'a': 200 }))
assert c.document_atomic_div('kv', 'k4', {'v': Document({'a' : 1})}) == True
assertEquals(c.get('kv', 'k4')['v'], Document({ 'a': 200 }))
assert c.document_atomic_div('kv', 'k4', {'v': Document({'a' : 2})}) == True
assertEquals(c.get('kv', 'k4')['v'], Document({ 'a': 100 }))
assert c.document_atomic_mul('kv', 'k4', {'v': Document({'a' : 4})}) == True
assertEquals(c.get('kv', 'k4')['v'], Document({ 'a': 400 }))
assert c.document_atomic_mul('kv', 'k4', {'v': Document({'a' : 1})}) == True
assertEquals(c.get('kv', 'k4')['v'], Document({ 'a': 400 }))
assert c.document_atomic_mul('kv', 'k4', {'v': Document({'a' : 0})}) == True
assertEquals(c.get('kv', 'k4')['v'], Document({ 'a': 0 }))

# Build a new subdocument
assert c.put('kv', 'k6', {'v' : Document({'a' : 100})})
assertEquals(c.get('kv', 'k6')['v'], Document({ 'a': 100 }))
assert c.document_atomic_add('kv', 'k6',  {'v': Document({'a': {'b' :1}})}) == False
assertEquals(c.get('kv', 'k6')['v'], Document({ 'a': 100 }))
assert c.document_atomic_add('kv', 'k6',  {'v': Document({'c': {'b' :1}})}) == True
assertEquals(c.get('kv', 'k6')['v'], Document({ 'a': 100 , 'c' : {'b' :1}}))


#Lists
assert c.put('kv', 'k5', {'v': Document(['a', 'b', 'c'])}) == True
assertEquals(c.get('kv', 'k5')['v'], Document(['a', 'b', 'c']))

# Test loading a big json file
json_file = open(os.getcwd() + '/test/test-data/big.json')
data = json.load(json_file)
assert c.put('kv', 'k2', {'v': Document(data)}) == True
assertEquals(c.get('kv', 'k2')['v'], Document(data))
