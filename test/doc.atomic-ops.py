# File generated from python blocks in "doc/atomic-ops.tex"
>>> import sys
>>> HOST = sys.argv[2]
>>> PORT = int(sys.argv[3])
>>> import hyperdex.admin
>>> a = hyperdex.admin.Admin(HOST, PORT)
>>> a.add_space('''
... space friendlists
... key username
... attributes
...    string first,
...    string last,
...    set(string) friends
... ''')
True
>>> import hyperdex.client
>>> c = hyperdex.client.Client(HOST, PORT)
>>> c.put('friendlists', 'jsmith1', {'first': 'John', 'last': 'Smith',
...                                  'friends': set(['bjones1', 'jd', 'jj'])})
True
>>> c.put('friendlists', 'jd', {'first': 'John', 'last': 'Doe'})
True
>>> c.put('friendlists', 'bjones1', {'first': 'Brian', 'last': 'Jones'})
True
>>> c.get('friendlists', 'jsmith1')
{'first': 'John', 'last': 'Smith', 'friends': set(['bjones1', 'jd', 'jj'])}
>>> c.cond_put('friendlists', 'jsmith1',
...            {'first': 'John', 'last': 'Smith'},
...            {'first': 'Jon'})
True
>>> c.get('friendlists', 'jsmith1')
{'first': 'Jon', 'last': 'Smith', 'friends': set(['bjones1', 'jd', 'jj'])}
>>> c.cond_put('friendlists', 'jsmith1',
...            {'first': 'John', 'last': 'Smith'},
...            {'first': 'Jon'})
False
>>> c.cond_put('friendlists', 'jsmith1',
...            {'friends': set(['bjones1', 'jd', 'jj'])},
...            {'first': 'John'})
True
>>> c.get('friendlists', 'jsmith1')
{'first': 'John', 'last': 'Smith', 'friends': set(['bjones1', 'jd', 'jj'])}
>>> a.add_space('''
... space userlocks
... key username
... ''')
True
>>> c.put_if_not_exist('userlocks', 'jsmith1', {})
True
>>> c.get('userlocks', 'jsmith1')
{}
>>> c.put_if_not_exist('userlocks', 'jsmith1', {})
False
>>> c.delete('userlocks', 'jsmith1')
True
>>> def lock(client, user):
...     while not client.put_if_not_exist('userlocks', user, {}):
...         pass
>>> def unlock(client, user):
...     client.delete('userlocks', user)
>>> lock(c, 'jsmith1')
>>> unlock(c, 'jsmith1')
>>> a.add_space('''
... space alldatatypes
... key k
... attributes
...    string s,
...    int i,
...    float f,
...    list(string) ls,
...    set(string) ss,
...    map(string, string) mss,
...    map(string, int) msi''')
True
>>> c.put_if_not_exist('alldatatypes', 'somekey', {'s': 'initial value'})
True
>>> c.put_if_not_exist('alldatatypes', 'somekey', {'s': 'initial value'})
False

>>> # cond_put.  First is predicate.  May be any valid search predicate
>>> c.cond_put('alldatatypes', 'somekey', {'s': 'initial value'}, {'s': 'some string'})
True
>>> c.cond_put('alldatatypes', 'somekey', {'s': 'initial value'}, {'s': 'some string'})
False
>>> c.get('alldatatypes', 'somekey')
{'f': 0.0, 'i': 0, 'mss': {}, 'ss': set([]), 's': 'some string', 'ls': [], 'msi': {}}
>>> c.cond_put_or_create('alldatatypes', 'anotherkey', {'s': 'a'}, {'s': 'b'})
True
>>> c.cond_put_or_create('alldatatypes', 'anotherkey', {'s': 'a'}, {'s': 'b'})
False
>>> c.get('alldatatypes', 'anotherkey')
{'f': 0.0, 'i': 0, 'mss': {}, 'ss': set([]), 's': 'b', 'ls': [], 'msi': {}}
>>> c.cond_put_or_create('alldatatypes', 'anotherkey', {'s': 'b'}, {'s': 'a'})
True
>>> c.get('alldatatypes', 'anotherkey')
{'f': 0.0, 'i': 0, 'mss': {}, 'ss': set([]), 's': 'a', 'ls': [], 'msi': {}}
>>> c.atomic_add('alldatatypes', 'somekey', {'i': 1, 'f': 0.25})
True
>>> c.get('alldatatypes', 'somekey')
{'f': 0.25, 'i': 1, 'mss': {}, 'ss': set([]), 's': 'some string', 'ls': [], 'msi': {}}

>>> c.atomic_sub('alldatatypes', 'somekey', {'i': 2, 'f': 0.5})
True
>>> c.get('alldatatypes', 'somekey')
{'f': -0.25, 'i': -1, 'mss': {}, 'ss': set([]), 's': 'some string', 'ls': [], 'msi': {}}

>>> c.atomic_mul('alldatatypes', 'somekey', {'i': 2, 'f': 4.})
True
>>> c.get('alldatatypes', 'somekey')
{'f': -1.0, 'i': -2, 'mss': {}, 'ss': set([]), 's': 'some string', 'ls': [], 'msi': {}}

>>> c.atomic_div('alldatatypes', 'somekey', {'i': 2, 'f': 4.})
True
>>> c.get('alldatatypes', 'somekey')
{'f': -0.25, 'i': -1, 'mss': {}, 'ss': set([]), 's': 'some string', 'ls': [], 'msi': {}}

>>> c.put('alldatatypes', 'somekey', {'i': 0xdeadbeefcafe})
True
>>> c.atomic_and('alldatatypes', 'somekey', {'i': 0xffffffff0000})
True
>>> c.get('alldatatypes', 'somekey')
{'f': -0.25, 'i': 244837814042624, 'mss': {}, 'ss': set([]), 's': 'some string', 'ls': [], 'msi': {}}
>>> print "0x%x" % (c.get('alldatatypes', 'somekey')['i'],)
0xdeadbeef0000

>>> c.atomic_or('alldatatypes', 'somekey', {'i': 0x00000000cafe})
True
>>> print "0x%x" % (c.get('alldatatypes', 'somekey')['i'],)
0xdeadbeefcafe

>>> c.atomic_xor('alldatatypes', 'somekey', {'i': 0xdea5a0403af3})
True
>>> print "0x%x" % (c.get('alldatatypes', 'somekey')['i'],)
0x81eaff00d
>>> c.string_prepend('alldatatypes', 'somekey', {'s': '->'})
True
>>> c.get('alldatatypes', 'somekey')['s']
'->some string'

>>> c.string_append('alldatatypes', 'somekey', {'s': '<-'})
True
>>> c.get('alldatatypes', 'somekey')['s']
'->some string<-'
>>> c.put('alldatatypes', 'somekey', {'ls': ['B']})
True
>>> c.list_lpush('alldatatypes', 'somekey', {'ls': 'A'})
True
>>> c.get('alldatatypes', 'somekey')['ls']
['A', 'B']

>>> c.list_rpush('alldatatypes', 'somekey', {'ls': 'C'})
True
>>> c.get('alldatatypes', 'somekey')['ls']
['A', 'B', 'C']
>>> c.set_add('alldatatypes', 'somekey', {'ss': 'C'})
True
>>> c.get('alldatatypes', 'somekey')['ss']
set(['C'])

>>> c.set_remove('alldatatypes', 'somekey', {'ss': 'C'})
True
>>> c.get('alldatatypes', 'somekey')['ss']
set([])

>>> c.set_union('alldatatypes', 'somekey', {'ss': set(['A', 'B', 'C'])})
True
>>> c.get('alldatatypes', 'somekey')['ss']
set(['A', 'C', 'B'])

>>> c.set_intersect('alldatatypes', 'somekey', {'ss': set(['A', 'B', 'Z'])})
True
>>> c.get('alldatatypes', 'somekey')['ss']
set(['A', 'B'])
>>> c.map_add('alldatatypes', 'somekey', {'mss': {'mapkey': 'mapvalue'}, 'msi': {'mapkey': 16}})
True
>>> c.get('alldatatypes', 'somekey')
{'f': -0.25, 'i': 34874585101, 'mss': {'mapkey': 'mapvalue'}, 'ss': set(['A', 'B']), 's': '->some string<-', 'ls': ['A', 'B', 'C'], 'msi': {'mapkey': 16}}
>>> c.map_add('alldatatypes', 'somekey', {'mss': {'tmp': 'delete me'}})
True
>>> c.get('alldatatypes', 'somekey')
{'f': -0.25, 'i': 34874585101, 'mss': {'tmp': 'delete me', 'mapkey': 'mapvalue'}, 'ss': set(['A', 'B']), 's': '->some string<-', 'ls': ['A', 'B', 'C'], 'msi': {'mapkey': 16}}
>>> c.map_remove('alldatatypes', 'somekey', {'mss': 'tmp'})
True
>>> c.get('alldatatypes', 'somekey')
{'f': -0.25, 'i': 34874585101, 'mss': {'mapkey': 'mapvalue'}, 'ss': set(['A', 'B']), 's': '->some string<-', 'ls': ['A', 'B', 'C'], 'msi': {'mapkey': 16}}
>>> c.map_atomic_add('alldatatypes', 'somekey', {'msi': {'mapkey': 16}})
True
>>> c.get('alldatatypes', 'somekey')
{'f': -0.25, 'i': 34874585101, 'mss': {'mapkey': 'mapvalue'}, 'ss': set(['A', 'B']), 's': '->some string<-', 'ls': ['A', 'B', 'C'], 'msi': {'mapkey': 32}}

>>> c.map_atomic_sub('alldatatypes', 'somekey', {'msi': {'mapkey': -32}})
True
>>> c.get('alldatatypes', 'somekey')
{'f': -0.25, 'i': 34874585101, 'mss': {'mapkey': 'mapvalue'}, 'ss': set(['A', 'B']), 's': '->some string<-', 'ls': ['A', 'B', 'C'], 'msi': {'mapkey': 64}}

>>> c.map_atomic_mul('alldatatypes', 'somekey', {'msi': {'mapkey': 4}})
True
>>> c.get('alldatatypes', 'somekey')
{'f': -0.25, 'i': 34874585101, 'mss': {'mapkey': 'mapvalue'}, 'ss': set(['A', 'B']), 's': '->some string<-', 'ls': ['A', 'B', 'C'], 'msi': {'mapkey': 256}}

>>> c.map_atomic_div('alldatatypes', 'somekey', {'msi': {'mapkey': 64}})
True
>>> c.get('alldatatypes', 'somekey')
{'f': -0.25, 'i': 34874585101, 'mss': {'mapkey': 'mapvalue'}, 'ss': set(['A', 'B']), 's': '->some string<-', 'ls': ['A', 'B', 'C'], 'msi': {'mapkey': 4}}

>>> c.map_atomic_and('alldatatypes', 'somekey', {'msi': {'mapkey': 2}})
True
>>> c.get('alldatatypes', 'somekey')
{'f': -0.25, 'i': 34874585101, 'mss': {'mapkey': 'mapvalue'}, 'ss': set(['A', 'B']), 's': '->some string<-', 'ls': ['A', 'B', 'C'], 'msi': {'mapkey': 0}}

>>> c.map_atomic_or('alldatatypes', 'somekey', {'msi': {'mapkey': 5}})
True
>>> c.get('alldatatypes', 'somekey')
{'f': -0.25, 'i': 34874585101, 'mss': {'mapkey': 'mapvalue'}, 'ss': set(['A', 'B']), 's': '->some string<-', 'ls': ['A', 'B', 'C'], 'msi': {'mapkey': 5}}

>>> c.map_atomic_xor('alldatatypes', 'somekey', {'msi': {'mapkey': 7}})
True
>>> c.get('alldatatypes', 'somekey')
{'f': -0.25, 'i': 34874585101, 'mss': {'mapkey': 'mapvalue'}, 'ss': set(['A', 'B']), 's': '->some string<-', 'ls': ['A', 'B', 'C'], 'msi': {'mapkey': 2}}

>>> c.map_string_prepend('alldatatypes', 'somekey', {'mss': {'mapkey': '->'}})
True
>>> c.get('alldatatypes', 'somekey')
{'f': -0.25, 'i': 34874585101, 'mss': {'mapkey': '->mapvalue'}, 'ss': set(['A', 'B']), 's': '->some string<-', 'ls': ['A', 'B', 'C'], 'msi': {'mapkey': 2}}

>>> c.map_string_append('alldatatypes', 'somekey', {'mss': {'mapkey': '<-'}})
True
>>> c.get('alldatatypes', 'somekey')
{'f': -0.25, 'i': 34874585101, 'mss': {'mapkey': '->mapvalue<-'}, 'ss': set(['A', 'B']), 's': '->some string<-', 'ls': ['A', 'B', 'C'], 'msi': {'mapkey': 2}}
>>> a.add_space('''
... space people
... key k
... attributes
...    document info''')
True
>>> Document = hyperdex.client.Document
>>> c.put('people', 'jane', {'info' : Document( {'name': 'Jane Doe', 'gender' : 'female', 'age' : 21, 'likes' : ['cornell', 'python']} )})
True
>>> c.atomic_add('people', 'jane', {'info.age' : 1})
True
>>> c.get('people', 'jane')
{'info': Document({"name": "Jane Doe", "gender": "female", "age": 22, "likes": ["cornell", "python"]})}
>>> c.atomic_add('people', 'jane', {'info.gender' : 1})
False
>>> c.atomic_add('people', 'jane', {'info.children' : 1})
True
>>> c.get('people', 'jane')
{'info': Document({"name": "Jane Doe", "gender": "female", "age": 22, "children": 1, "likes": ["cornell", "python"]})}
>>> c.string_prepend('people', 'jane', {'info.name' : 'Dr. '})
True
>>> c.get('people', 'jane')
{'info': Document({"name": "Dr. Jane Doe", "gender": "female", "age": 22, "children": 1, "likes": ["cornell", "python"]})}
>>> c.string_append('people', 'jane', {'info.name' : ', Jr.'})
True
>>> c.get('people', 'jane')
{'info': Document({"name": "Dr. Jane Doe, Jr.", "gender": "female", "age": 22, "children": 1, "likes": ["cornell", "python"]})}
