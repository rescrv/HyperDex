#!/usr/bin/env python2
import sys
import hyperdex.client
import json
import os
from testlib import *
from hyperdex.client import *

c = hyperdex.client.Client(sys.argv[1], int(sys.argv[2]))

def to_objectset(xs):
    return set([frozenset(x.items()) for x in xs])

assertTrue(c.put('kv', 'k', {'v': Document({})}))
assertEquals(c.get('kv', 'k')['v'], Document({}))
assertTrue(c.put('kv', 'k',  {'v': Document({'a': 'b', 'c': {'d' : 1, 'e': 'f', 'g': -2 }})}))
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'b', 'c': {'d' : 1, 'e': 'f', 'g': -2 }}))

assertFalse(c.atomic_add('kv', 'k',  {'v.a': 1}))
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'b', 'c': {'d' : 1, 'e': 'f', 'g': -2 }}))
assertTrue(c.atomic_add('kv', 'k',  {'v.c.d' : 5}))
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'b', 'c': {'d' : 6, 'e': 'f', 'g': -2 }}))
assertTrue(c.atomic_add('kv', 'k',  {'v.c.d' : 5, 'v.c.g': 5}))
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'b', 'c': {'d' : 11, 'e': 'f' , 'g': 3}}))
assertTrue(c.string_prepend('kv', 'k',  {'v.a' : 'x', 'v.c.e': 'z'}))
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'xb', 'c': {'d' : 11, 'e': 'zf', 'g': 3}}))
assertTrue(c.string_append('kv', 'k',  {'v.a' : 'x', 'v.c.e': 'z'}))
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'xbx', 'c': {'d' : 11, 'e': 'zfz', 'g': 3}}))
assertTrue(c.string_append('kv', 'k',  {'v.k.l': 'm'}))
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'xbx', 'c': {'d' : 11, 'e': 'zfz', 'g': 3}, 'k' : {'l' : 'm'}}))
assertTrue(c.atomic_add('kv', 'k',  {'v.k.a.b.c.d' : 1}))
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'xbx', 'c': {'d' : 11, 'e': 'zfz', 'g': 3}, 'k' : {'a': {'b' : {'c' : {'d' : 1}}}, 'l' : 'm'}}))
assertTrue(c.atomic_sub('kv', 'k',  {'v.k.a.b.c.d' : 5}))
assertEquals(c.get('kv', 'k')['v'], Document({'a': 'xbx', 'c': {'d' : 11, 'e': 'zfz', 'g': 3}, 'k' : {'a': {'b' : {'c' : {'d' : -4}}}, 'l' : 'm'}}))

# More exotic operations
assertTrue(c.put('kv', 'k3',  {'v': Document({'a': 'b', 'c': {'d' : 100, 'e': 'f', 'g': 5 }})}))
assertEquals(c.get('kv', 'k3')['v'], Document({'a': 'b', 'c': {'d' : 100, 'e': 'f', 'g': 5 }}))
assertTrue(c.atomic_mod('kv', 'k3', {'v.c.d' : 10000}))
assertEquals(c.get('kv', 'k3')['v'], Document({'a': 'b', 'c': {'d' : 100, 'e': 'f', 'g': 5 }}))
assertTrue(c.atomic_mod('kv', 'k3', {'v.c.d' : 22}))
assertEquals(c.get('kv', 'k3')['v'], Document({'a': 'b', 'c': {'d' : 12, 'e': 'f', 'g': 5 }}))
assertTrue(c.atomic_xor('kv', 'k3', {'v.c.g' : 4}))
assertEquals(c.get('kv', 'k3')['v'], Document({'a': 'b', 'c': {'d' : 12, 'e': 'f', 'g': 1 }}))
assertTrue(c.atomic_or('kv', 'k3', {'v.c.g' : 4}))
assertEquals(c.get('kv', 'k3')['v'], Document({'a': 'b', 'c': {'d' : 12, 'e': 'f', 'g': 5 }}))

# Multiply and divide
assertTrue(c.put('kv', 'k4',  {'v': Document({'a': 200})}))
assertEquals(c.get('kv', 'k4')['v'], Document({ 'a': 200 }))
assertTrue(c.atomic_div('kv', 'k4', {'v.a' : 1}))
assertEquals(c.get('kv', 'k4')['v'], Document({ 'a': 200 }))
assertTrue(c.atomic_div('kv', 'k4', {'v.a' : 2}))
assertEquals(c.get('kv', 'k4')['v'], Document({ 'a': 100 }))
assertTrue(c.atomic_mul('kv', 'k4', {'v.a' : 4}))
assertEquals(c.get('kv', 'k4')['v'], Document({ 'a': 400 }))
assertTrue(c.atomic_mul('kv', 'k4', {'v.a' : 1}))
assertEquals(c.get('kv', 'k4')['v'], Document({ 'a': 400 }))
assertTrue(c.atomic_mul('kv', 'k4', {'v.a' : 0}))
assertEquals(c.get('kv', 'k4')['v'], Document({ 'a': 0 }))

# Build a new subdocument
assertTrue(c.put('kv', 'k6', {'v' : Document({'a' : 100})}))
assertEquals(c.get('kv', 'k6')['v'], Document({ 'a': 100 }))
assertFalse(c.atomic_add('kv', 'k6',  {'v.a.b' :1}))
assertEquals(c.get('kv', 'k6')['v'], Document({ 'a': 100 }))
assertTrue(c.atomic_add('kv', 'k6',  {'v.c.b' :1}))
assertEquals(c.get('kv', 'k6')['v'], Document({ 'a': 100 , 'c' : {'b' :1}}))
assertTrue(c.string_prepend('kv', 'k6',  {'v.i.j.k' : 'xyz'}))
assertEquals(c.get('kv', 'k6')['v'], Document({ 'a': 100 , 'c' : {'b' :1}, 'i' : {'j' : {'k' : 'xyz'}}}))
assertFalse(c.string_prepend('kv', 'k6',  {'v.i.j' : 'xyz'}))
assertEquals(c.get('kv', 'k6')['v'], Document({ 'a': 100 , 'c' : {'b' :1}, 'i' : {'j' : {'k' : 'xyz'}}}))

#Lists
#FIXME unsupported by libbson
#assertTrue(c.put('kv', 'k5', {'v': Document(['a', 'b', 'c'])}))
#assertEquals(c.get('kv', 'k5')['v'], Document(['a', 'b', 'c']))

# Test loading a big json file
#FIXME arrays unsupported by libbson. maybe use a different test document?
#json_file = open(os.getcwd() + '/test/test-data/big.json')
#data = json.load(json_file)
#assertTrue(c.put('kv', 'k2', {'v': Document(data)}))
#assertEquals(c.get('kv', 'k2')['v'], Document(data))

# Remove a property
assertTrue(c.put('kv', 'k7', {'v' : Document({'a' : {'b' : 3}})}))
assertEquals(c.get('kv', 'k7')['v'], Document({'a' : {'b' : 3}}))
assertTrue(c.document_unset('kv', 'k7', {'v.a.b' : 1}))
assertEquals(c.get('kv', 'k7')['v'], Document({'a' : {}}))
assertFalse(c.document_unset('kv', 'k7', {'v.a.b' : 1}))

# Rename a property
assertTrue(c.put('kv', 'k7', {'v' : Document({'a' : {'b' : 3}})}))
assertEquals(c.get('kv', 'k7')['v'], Document({'a' : {'b' : 3}}))
assertTrue(c.document_rename('kv', 'k7', {'v.a.b' : 'c'}))
assertEquals(c.get('kv', 'k7')['v'], Document({'a' : {'c' : 3}}))
assertFalse(c.document_rename('kv', 'k7', {'v.a.b' : 'c'}))
