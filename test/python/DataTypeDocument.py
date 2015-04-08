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
    
# Empty Document
assertTrue(c.put('kv', 'k', {}))
assertEquals(c.get('kv', 'k')['v'], Document({}))

# Basic Stuff
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

# Bit operations
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

# Floating point numbers
assertTrue(c.put('kv', 'k10',  {'v': Document({'a': 200})}))
assertTrue(c.atomic_add('kv', 'k10', {'v.a' : 100.0}))
assertEquals(c.get('kv', 'k10')['v'], Document({ 'a': 300.0 }))
assertTrue(c.atomic_mul('kv', 'k10', {'v.a' : 1.5}))
assertEquals(c.get('kv', 'k10')['v'], Document({ 'a': 450.0 }))

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
assertTrue(c.put('kv', 'k6', {'v.d' : Document({'q' : 1})}))
assertEquals(c.get('kv', 'k6')['v'], Document({ 'a': 100 , 'c' : {'b' :1}, 'i' : {'j' : {'k' : 'xyz'}}, 'd' : {'q' : 1}}))

# Remove a property
assertTrue(c.put('kv', 'k7', {'v' : Document({'a' : {'b' : 3}})}))
assertEquals(c.get('kv', 'k7')['v'], Document({'a' : {'b' : 3}}))
assertTrue(c.document_unset('kv', 'k7', {'v.a.b' : 1}))
assertEquals(c.get('kv', 'k7')['v'], Document({'a' : {}}))
assertFalse(c.document_unset('kv', 'k7', {'v.a.b' : 1}))

# Rename a property
assertTrue(c.put('kv', 'k7', {'v' : Document({'a' : {'b' : 3}})}))
assertEquals(c.get('kv', 'k7')['v'], Document({'a' : {'b' : 3}}))
assertTrue(c.document_rename('kv', 'k7', {'v.a.b' : 'a.c'}))
assertEquals(c.get('kv', 'k7')['v'], Document({'a' : {'c' : 3}}))
assertFalse(c.document_rename('kv', 'k7', {'v.a.b' : 'c'}))
assertFalse(c.document_rename('kv', 'k7', {'v.a.b' : 'b'}))

# Set new values (returns false if they already exist)
assertTrue(c.put('kv', 'k8', {'v' : Document({'a' : { 'b' : 'c'}})}))
assertEquals(c.get('kv', 'k8')['v'], Document({'a' : {'b' : 'c'}}))
assertTrue(c.put('kv', 'k8', {'v.a.b' : 'c'}))
assertEquals(c.get('kv', 'k8')['v'], Document({'a' : {'b' : 'c'}}))
assertTrue(c.put('kv', 'k8', {'v.a.b' : 'c'}))
assertEquals(c.get('kv', 'k8')['v'], Document({'a' : {'b' : 'c'}}))
assertTrue(c.put('kv', 'k8', {'v.a.c' :  1}))
assertEquals(c.get('kv', 'k8')['v'], Document({'a' : {'b' : 'c', 'c' : 1}}))
assertTrue(c.put('kv', 'k8', {'v.a.c' : 2}))
assertEquals(c.get('kv', 'k8')['v'], Document({'a' : {'b' : 'c', 'c' : 2}}))
assertTrue(c.put('kv', 'k8', {'v.a.c' : 'c'}))
assertEquals(c.get('kv', 'k8')['v'], Document({'a' : {'b' : 'c', 'c' : 'c'}}))
assertTrue(c.put('kv', 'k8', {'v.b.a' : 1, 'v.b.b' : 1, 'v.b.c' : 'xyz'}))
assertEquals(c.get('kv', 'k8')['v'], Document({'a' : {'b' : 'c', 'c' : 'c'}, 'b' : {'a' : 1, 'b' : 1, 'c' : 'xyz'}}))
assertTrue(c.put('kv', 'k8', {'v.c' : Document({'b' : {'a' : 1, 'b' : 1, 'c' : 'xyz'}})}))
assertEquals(c.get('kv', 'k8')['v'], Document({'a' : {'b' : 'c', 'c' : 'c'}, 'b' : {'a' : 1, 'b' : 1, 'c' : 'xyz'}, 'c' : {'b' : {'a' : 1, 'b' : 1, 'c' : 'xyz'}}}))
assertTrue(c.put('kv', 'k8', {'v.d' : 2.5}))
assertEquals(c.get('kv', 'k8')['v'], Document({'a' : {'b' : 'c', 'c' : 'c'}, 'b' : {'a' : 1, 'b' : 1, 'c' : 'xyz'}, 'c' : {'b' : {'a' : 1, 'b' : 1, 'c' : 'xyz'}}, 'd' : 2.5}))

# Arrays
assertTrue(c.put('kv', 'k11', {'v' : Document({'a' : [1,2,3,0]})}))
assertEquals(c.get('kv', 'k11')['v'], Document({'a' : [1,2,3,0]}))
assertTrue(c.put('kv', 'k11', {'v.a[3]' : 4}))
assertEquals(c.get('kv', 'k11')['v'], Document({'a' : [1,2,3,4]}))
assertFalse(c.put('kv', 'k11', {'v.a[3].b' : 4}))
assertEquals(c.get('kv', 'k11')['v'], Document({'a' : [1,2,3,4]}))
assertTrue(c.list_rpush('kv', 'k11', {'v.a' : "5"}))
assertEquals(c.get('kv', 'k11')['v'], Document({'a' : [1,2,3,4,"5"]}))
assertTrue(c.list_rpush('kv', 'k11', {'v.a' : Document({'x':'y'})}))
assertEquals(c.get('kv', 'k11')['v'], Document({'a' : [1,2,3,4,"5",{'x':'y'}]}))
assertTrue(c.list_lpush('kv', 'k11', {'v.a' : 0}))
assertEquals(c.get('kv', 'k11')['v'], Document({'a' : [0,1,2,3,4,"5",{'x':'y'}]}))

# Search on Documents
assertTrue(c.put('kv', 'k9', {'v' : Document({'x' : {'b' : 'c'}})}))
res1 = c.search('kv', {'v.x.b' : 'c'})
res2 = c.search('kv', {'v.x' : Document({'b' : 'c'})})
res3 = c.search('kv', {'v' : Document({'x' : {'b' : 'c'}})})
assertEquals(res1.next(), {'k' : 'k9', 'v' : Document({'x' : {'b' : 'c'}})})
assertFalse(res1.hasNext())
assertEquals(res2.next(), {'k' : 'k9', 'v' : Document({'x' : {'b' : 'c'}})})
assertFalse(res2.hasNext())
assertEquals(res3.next(), {'k' : 'k9', 'v' : Document({'x' : {'b' : 'c'}})})
assertFalse(res3.hasNext())

