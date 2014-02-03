# File generated from python blocks in "doc/async-ops.tex"
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
>>> c.put('friendlists', 'jsmith1', {'first': 'John', 'last': 'Smith'})
True
>>> c.put('friendlists', 'jd', {'first': 'John', 'last': 'Doe'})
True
>>> c.put('friendlists', 'bjones1', {'first': 'Brian', 'last': 'Jones'})
True
>>> d = c.async_put('friendlists', 'jj', {'first': 'John', 'last': 'Jackson'})
>>> d
<hyperdex.client.Deferred object at ...>
>>> # do some work
>>> d.wait()
True
>>> d = c.async_get('friendlists', 'jj')
>>> d.wait()
{'first': 'John', 'last': 'Jackson', 'friends': set([])}
>>> def get(client, space, key):
...     return client.async_get(space, key).wait()
...
>>> get(c, 'friendlists', 'jj')
{'first': 'John', 'last': 'Jackson', 'friends': set([])}
>>> d1 = c.async_set_add('friendlists', 'jj', {'friends': 'jsmith1'})
>>> d2 = c.async_set_add('friendlists', 'jsmith1', {'friends': 'jj'})
>>> d1.wait()
True
>>> d2.wait()
True
>>> d1 = c.async_set_add('friendlists', 'jsmith1', {'friends': 'bjones1'})
>>> d2 = c.async_set_add('friendlists', 'bjones1', {'friends': 'jsmith1'})
>>> d3 = c.async_set_add('friendlists', 'jsmith1', {'friends': 'jd'})
>>> d4 = c.async_set_add('friendlists', 'jd', {'friends': 'jsmith1'})
>>> d1.wait()
True
>>> d2.wait()
True
>>> d3.wait()
True
>>> d4.wait()
True
>>> friends_usernames = c.get('friendlists', 'jsmith1')['friends']
>>> outstanding = set()
>>> for username in friends_usernames:
...     outstanding.add(c.async_get('friendlists', username))
...
>>> friends = []
>>> while outstanding:
...     d = c.loop()
...     outstanding.remove(d)
...     friend = d.wait()['first']
...     friends.append(friend)
...
>>> sorted(friends)
['Brian', 'John', 'John']
